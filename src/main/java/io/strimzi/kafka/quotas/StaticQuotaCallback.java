/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private final AtomicLong storageUsed = new AtomicLong(0);
    private final AtomicReference<Map<FileStore, Double>> storagePercentages = new AtomicReference<>(Collections.emptyMap());
    private volatile List<Path> logDirs;
    private volatile Map<FileStore, Threshold> thresholdMap = Map.of(); // start with empty, so we don't get NPE
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile int storageCheckInterval = Integer.MAX_VALUE;
    private final AtomicBoolean resetQuota = new AtomicBoolean(false);
    final StorageChecker storageChecker = new StorageChecker();
    private final static long LOGGING_DELAY_MS = 1000;
    private final AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private final AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);
    private final AtomicLong lastLoggedMessagePercentTimeMs = new AtomicLong(0);

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        double throttle = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
        // Don't allow producing messages if we're beyond the storage limit.
        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            Map<FileStore, Double> volumePercents = storagePercentages.get();
            for (Map.Entry<FileStore, Double> entry : volumePercents.entrySet()) {
                Threshold threshold = thresholdMap.get(entry.getKey());
                if (threshold != null) {
                    Double currentPercent = entry.getValue();
                    Double softPercent = threshold.soft;
                    Double hardPercent = threshold.hard;
                    if (currentPercent >= hardPercent) {
                        maybeLog(lastLoggedMessagePercentTimeMs, "Limiting producer rate because a volume is full. Used percent: {}. Limit percent: {}", currentPercent, hardPercent);
                        return 1.0;
                    } else if (currentPercent > softPercent) {
                        double limit = throttle * (1.0 - ((currentPercent - softPercent) / (hardPercent - softPercent)));
                        maybeLog(lastLoggedMessagePercentTimeMs, "Throttling producer rate because disk is beyond soft percent limit. Used percent: {}. Quota: {}", currentPercent, limit);
                        return limit;
                    }
                }
            }

            long currentStorageUsage = storageUsed.get();
            if (currentStorageUsage >= storageQuotaHard) {
                maybeLog(lastLoggedMessageHardTimeMs, "Limiting producer rate because disk is full. Used: {}. Limit: {}", storageUsed, storageQuotaHard);
                return 1.0;
            } else if (currentStorageUsage > storageQuotaSoft) {
                double limit = throttle * (1.0 - (1.0 * (currentStorageUsage - storageQuotaSoft) / (storageQuotaHard - storageQuotaSoft)));
                maybeLog(lastLoggedMessageSoftTimeMs, "Throttling producer rate because disk is beyond soft limit. Used: {}. Quota: {}", storageUsed, limit);
                return limit;
            }
        }
        return throttle;
    }

    /**
     * Put a small delay between logging
     */
    private void maybeLog(AtomicLong lastLoggedMessageTimeMs, String format, Object... args) {
        if (log.isDebugEnabled()) {
            long now = System.currentTimeMillis();
            final boolean[] shouldLog = {true};
            lastLoggedMessageTimeMs.getAndUpdate(current -> {
                if (now - current >= LOGGING_DELAY_MS) {
                    shouldLog[0] = true;
                    return now;
                }
                shouldLog[0] = false;
                return current;
            });
            if (shouldLog[0]) {
                log.debug(format, args);
            }
        }
    }

    @Override
    public void updateQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity, double newValue) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public void removeQuota(ClientQuotaType quotaType, ClientQuotaEntity quotaEntity) {
        // Unused: This plugin does not care about user or client id entities.
    }

    @Override
    public boolean quotaResetRequired(ClientQuotaType quotaType) {
        return resetQuota.getAndSet(true);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        storageChecker.startIfNecessary();
        return false;
    }

    @Override
    public void close() {
        try {
            storageChecker.stop();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        if (storageQuotaSoft > storageQuotaHard) {
            throw new InvalidConfigurationException("Soft limit is bigger than hard limit");
        }
        storageCheckInterval = config.getStorageCheckInterval();
        if (storageCheckInterval < 0) {
            throw new InvalidConfigurationException("Check interval cannot be negative");
        }
        logDirs = Arrays.stream(config.getLogDirs().split(",")).map(Paths::get).collect(Collectors.toList());
        Map<FileStore, Threshold> tmp = new HashMap<>();
        String percents = config.getStoragePercents();
        String[] split = percents.split("\\|");
        for (String s : split) {
            String[] inner = s.split(";");
            Path path = Paths.get(inner[0]);
            if (!logDirs.contains(path)) {
                throw new InvalidConfigurationException(StaticQuotaConfig.STORAGE_QUOTA_PERCENTAGES_PROP + " property contains path that's not part of logDirs: " + path + " - " + logDirs);
            }
            double soft = Double.parseDouble(inner[1]);
            double hard = Double.parseDouble(inner[2]);
            if ((soft < 0) || (hard < 0) || (hard <= soft) || (soft > 100) || (hard > 100)) {
                throw new InvalidConfigurationException("Invalid soft or hard percent limit: " + s);
            }
            FileStore store = apply(() -> Files.getFileStore(path));
            tmp.compute(store, (f, e) -> {
                if (e == null) {
                    return new Threshold(soft, hard);
                } else {
                    log.warn("Duplicate volume found: " + f);
                    return new Threshold(Math.min(e.soft, soft), Math.max(e.hard, hard));
                }
            });
        }
        thresholdMap = tmp;
        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Percents map: {}. Storage check interval: {}", quotaMap, storageQuotaSoft, storageQuotaHard, thresholdMap, storageCheckInterval);
    }

    private final static class Threshold {
        final double soft;
        final double hard;

        public Threshold(double soft, double hard) {
            this.soft = soft;
            this.hard = hard;
        }
    }

    class StorageChecker implements Runnable {
        private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final String scope = "io.strimzi.kafka.quotas.StaticQuotaCallback";

        private void createCustomMetrics() {
            Metrics.newGauge(metricName("TotalStorageUsedBytes"), new Gauge<Long>() {
                public Long value() {
                    return storageUsed.get();
                }
            });
            Metrics.newGauge(metricName("SoftLimitBytes"), new Gauge<Long>() {
                public Long value() {
                    return storageQuotaSoft;
                }
            });
        }

        private MetricName metricName(String name) {
            String mBeanName = "io.strimzi.kafka.quotas:type=StorageChecker,name=" + name + "";
            return new MetricName("io.strimzi.kafka.quotas", "StorageChecker", name, this.scope, mBeanName);
        }

        void startIfNecessary() {
            if (running.compareAndSet(false, true)) {
                createCustomMetrics();
                storageCheckerThread.setDaemon(true);
                storageCheckerThread.start();
            }
        }

        void stop() throws InterruptedException {
            running.set(false);
            storageCheckerThread.interrupt();
            storageCheckerThread.join();
        }

        @Override
        public void run() {
            if (StaticQuotaCallback.this.logDirs != null
                && StaticQuotaCallback.this.storageQuotaSoft > 0
                && StaticQuotaCallback.this.storageQuotaHard > 0
                && StaticQuotaCallback.this.storageCheckInterval > 0) {
                try {
                    log.info("Quota Storage Checker is now starting");
                    while (running.get()) {
                        try {
                            Map<FileStore, Double> volumePercents = new HashMap<>();
                            long diskUsage = checkDiskUsage(volumePercents);
                            storagePercentages.set(volumePercents);
                            long previousUsage = StaticQuotaCallback.this.storageUsed.getAndSet(diskUsage);
                            if (diskUsage != previousUsage) {
                                StaticQuotaCallback.this.resetQuota.set(true);
                            }
                            log.debug("Storage usage checked: {}", StaticQuotaCallback.this.storageUsed.get());
                            Thread.sleep(TimeUnit.SECONDS.toMillis(StaticQuotaCallback.this.storageCheckInterval));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception e) {
                            log.warn("Exception in storage checker thread", e);
                        }
                    }
                } finally {
                    log.info("Quota Storage Checker is now finishing");
                }
            }
        }

        long checkDiskUsage(Map<FileStore, Double> volumePercents) {
            return logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct()
                .mapToLong(store -> {
                    try {
                        long usableSpace = store.getUsableSpace();
                        long totalSpace = store.getTotalSpace();
                        long usedSpace = totalSpace - usableSpace;
                        volumePercents.put(store, 100.0 * usedSpace / totalSpace);
                        return usedSpace;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .sum();
        }
    }

    static <T> T apply(IOSupplier<T> supplier) {
        try {
            return supplier.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @FunctionalInterface
    interface IOSupplier<T> {
        T get() throws IOException;
    }
}
