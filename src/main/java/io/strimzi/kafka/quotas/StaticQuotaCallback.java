/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Gauge;

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
    private volatile String logDirs;
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile int storageCheckInterval = Integer.MAX_VALUE;
    private final AtomicBoolean resetQuota = new AtomicBoolean(false);
    private final StorageChecker storageChecker = new StorageChecker();

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
            log.debug("Throttling producer rate because disk is beyond soft limit. Used: {}. Quota: {}", storageUsed, limit);
            return limit;
        } else if (ClientQuotaType.PRODUCE.equals(quotaType) && currentStorageUsage >= storageQuotaHard) {
            log.debug("Limiting producer rate because disk is full. Used: {}. Limit: {}", storageUsed, storageQuotaHard);
            return 1.0;
        }
        return quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
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
        logDirs = config.getLogDirs();

        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckInterval);
    }

    private class StorageChecker implements Runnable {
        private final Thread storageCheckerThread = new Thread(this, "storage-quota-checker");
        private AtomicBoolean running = new AtomicBoolean(false);
        private final Metric kafkaBrokerAllUsedBytes = Metrics.newGauge(metricName("TotalStorageUsedBytes"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        private final Metric getKafkaBrokerSoftLimitBytes = Metrics.newGauge(metricName("SoftLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaSoft;
            }
        });

        private MetricName metricName(String name) {
            MetricName metricName = new MetricName(StaticQuotaCallback.class, name);
            return new MetricName(metricName.getGroup(),
                    metricName.getType(),
                    metricName.getName(),
                    metricName.getScope(),
                    metricName.getMBeanName().replace("\"", ""));
        }

        void startIfNecessary() {
            if (running.compareAndSet(false, true)) {
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

        private long checkDiskUsage() throws IOException, InterruptedException {
            List<String> dirList = Arrays.asList(logDirs.split(","));
            Set<FileStore> fileStores = new HashSet<>();
            for (String d : dirList) {
                fileStores.add(Files.getFileStore(Paths.get(d)));
            }

            long totalUsed = 0;
            for (FileStore store : fileStores) {
                long used = store.getTotalSpace() - store.getUsableSpace();
                totalUsed = totalUsed + used;
            }

            return totalUsed;
        }
    }
}
