/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.apache.kafka.common.Cluster;
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
    private volatile List<Path> logDirs;
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile int storageCheckInterval = Integer.MAX_VALUE;
    private final AtomicBoolean resetQuota = new AtomicBoolean(false);
    final StorageChecker storageChecker = new StorageChecker();
    private final static long LOGGING_DELAY_MS = 1000;
    private AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        // Don't allow producing messages if we're beyond the storage limit.
        long currentStorageUsage = storageUsed.get();
        if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage > storageQuotaSoft && currentStorageUsage < storageQuotaHard) {
            double minThrottle = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
            double limit = minThrottle * (1.0 - (1.0 * (currentStorageUsage - storageQuotaSoft) / (storageQuotaHard - storageQuotaSoft)));
            maybeLog(lastLoggedMessageSoftTimeMs, "Throttling producer rate because disk is beyond soft limit. Used: {}. Quota: {}", storageUsed, limit);
            return limit;
        } else if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage >= storageQuotaHard) {
            maybeLog(lastLoggedMessageHardTimeMs, "Limiting producer rate because disk is full. Used: {}. Limit: {}", storageUsed, storageQuotaHard);
            return 1.0;
        }
        return quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
    }

    /**
     * Put a small delay between logging
     */
    private void maybeLog(AtomicLong latLoggedMessageTimeMs, String format, Object... args) {
        long now = System.currentTimeMillis();
        if (now - latLoggedMessageTimeMs.get() >= LOGGING_DELAY_MS) {
            log.debug(format, args);
            latLoggedMessageTimeMs.set(now);
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
        storageCheckInterval = config.getStorageCheckInterval();
        logDirs = Arrays.stream(config.getLogDirs().split(",")).map(Paths::get).collect(Collectors.toList());

        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckInterval);
    }

    class StorageChecker implements Runnable {
        private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
        private AtomicBoolean running = new AtomicBoolean(false);
        private String scope = "io.strimzi.kafka.quotas.StaticQuotaCallback";

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
                            long diskUsage = checkDiskUsage();
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

        long checkDiskUsage() {
            return logDirs.stream()
                .filter(Files::exists)
                .map(path -> apply(() -> Files.getFileStore(path)))
                .distinct()
                .mapToLong(store -> apply(() -> store.getTotalSpace() - store.getUsableSpace()))
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
