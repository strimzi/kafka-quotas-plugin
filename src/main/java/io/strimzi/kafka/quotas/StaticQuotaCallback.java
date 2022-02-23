/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";

    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private final AtomicLong storageUsed = new AtomicLong(0);
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile int storageCheckInterval = Integer.MAX_VALUE;
    private volatile List<String> excludedPrincipalNameList = List.of();
    private final AtomicBoolean resetQuota = new AtomicBoolean(false);
    private final StorageChecker storageChecker = new StorageChecker();
    private final static long LOGGING_DELAY_MS = 1000;
    private AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);
    private final String scope = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    @Override
    public Map<String, String> quotaMetricTags(ClientQuotaType quotaType, KafkaPrincipal principal, String clientId) {
        Map<String, String> m = new HashMap<>();
        m.put("quota.type", quotaType.name());
        if (!excludedPrincipalNameList.isEmpty() && principal != null && excludedPrincipalNameList.contains(principal.getName())) {
            m.put(EXCLUDED_PRINCIPAL_QUOTA_KEY, Boolean.TRUE.toString());
        }
        return m;
    }

    @Override
    public Double quotaLimit(ClientQuotaType quotaType, Map<String, String> metricTags) {
        if (Boolean.TRUE.toString().equals(metricTags.get(EXCLUDED_PRINCIPAL_QUOTA_KEY))) {
            return Quota.upperBound(Double.MAX_VALUE).bound();
        }

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
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        List<Path> logDirs = config.getLogDirs().stream().map(Paths::get).collect(Collectors.toList());
        storageChecker.configure(config.getStorageCheckInterval(),
                logDirs,
                this::updateUsedStorage);

        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckInterval);
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        createCustomMetrics();
    }

    private void updateUsedStorage(Long newValue) {
        var oldValue = storageUsed.getAndSet(newValue);
        if (oldValue != newValue) {
            resetQuota.set(true);
        }
    }

    private MetricName metricName(String name) {
        String mBeanName = "io.strimzi.kafka.quotas:type=StorageChecker,name=" + name + "";
        return new MetricName("io.strimzi.kafka.quotas", "StorageChecker", name, this.scope, mBeanName);
    }

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
}
