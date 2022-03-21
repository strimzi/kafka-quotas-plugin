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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.policy.QuotaPolicy;
import io.strimzi.kafka.quotas.policy.UnlimitedQuotaPolicy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Locale.ENGLISH;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";
    public static final double EPSILON = 1E-5;

    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private final AtomicLong storageUsed = new AtomicLong(0);
    private volatile QuotaPolicy quotaPolicy = UnlimitedQuotaPolicy.INSTANCE;
    private volatile List<String> excludedPrincipalNameList = List.of();
    private final AtomicBoolean resetQuota = new AtomicBoolean(true);
    private final StorageChecker storageChecker;
    private final static long LOGGING_DELAY_MS = 1000;
    private final AtomicLong lastLoggedMessageSoftTimeMs = new AtomicLong(0);
    private final AtomicLong lastLoggedMessageHardTimeMs = new AtomicLong(0);
    private static final String SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";
    /* test */ volatile Double currentQuotaFactor = 1.0;

    public StaticQuotaCallback() {
        this(new StorageChecker());
    }

    StaticQuotaCallback(StorageChecker storageChecker) {
        this.storageChecker = storageChecker;
    }

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
        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            double minThrottle = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
            final double produceQuota = minThrottle * currentQuotaFactor;
            final double limit = Math.max(produceQuota, 1.0D);
            maybeLog(lastLoggedMessageSoftTimeMs, "Throttling producer rate because disk is beyond soft limit. Used: {}. Quota: {}", storageUsed.get(), limit);
            return limit;
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
        return resetQuota.getAndSet(false);
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
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> SCOPE.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        long storageCheckIntervalMillis = TimeUnit.SECONDS.toMillis(config.getStorageCheckInterval());
        List<Path> logDirs = config.getLogDirs().stream().map(Paths::get).collect(Collectors.toList());
        storageChecker.configure(storageCheckIntervalMillis, logDirs, this::calculateQuotaFactor);

        quotaPolicy = config.getQuotaPolicy();

        log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}ms", quotaMap, quotaPolicy.getSoftLimit(), quotaPolicy.getHardLimit(), storageCheckIntervalMillis);
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        Metrics.newGauge(metricName(StorageChecker.class, "TotalStorageUsedBytes"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        //TODO doesn't really make sense if the policy is percentage based. It could still make sense per volume
        Metrics.newGauge(metricName(StorageChecker.class, "SoftLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return quotaPolicy.getSoftLimit().longValue();
            }
        });
        //TODO doesn't really make sense if the policy is percentage based. It could still make sense per volume
        Metrics.newGauge(metricName(StorageChecker.class, "HardLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return quotaPolicy.getHardLimit().longValue();
            }
        });

        quotaMap.forEach((clientQuotaType, quota) -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(quota));
        });
    }

    /* test */ void calculateQuotaFactor(Map<String, VolumeDetails> usagePerDisk) {
        Double newFactor = 1.0D;
        for (Map.Entry<String, VolumeDetails> diskUsage : usagePerDisk.entrySet()) {
            if (quotaPolicy.breachesHardLimit(diskUsage.getValue())) {
                newFactor = 0.0D;
                maybeLog(lastLoggedMessageHardTimeMs, "Limiting producer rate because  {} is full. Used: {}. Limit: {}", diskUsage.getKey(), diskUsage.getValue(), quotaPolicy.getHardLimit());
                //If any disk is over the hard limit that hard limit is applied.
                break;
            } else if (quotaPolicy.breachesSoftLimit(diskUsage.getValue())) {
                //Apply the most aggressive factor across all the disks
                newFactor = Math.min(newFactor, quotaPolicy.quotaFactor(diskUsage.getValue()));
            }
        }
        storageUsed.set(storageChecker.totalDiskUsage(usagePerDisk));
        if (Math.abs(currentQuotaFactor - newFactor) > EPSILON) {
            currentQuotaFactor = newFactor;
            resetQuota.set(true);
        }
    }

    static MetricName metricName(Class<?> clazz, String name) {
        return metricName(clazz, name, Map.of());
    }

    static MetricName metricName(Class<?> clazz, String name, Map<String, String> labels) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        final String labelValues = labels.size() > 0 ? buildLabelString(labels) : "";
        String mBeanName = String.format("%s:type=%s,name=%s%s", group, type, name, labelValues);
        return new MetricName(group, type, name, SCOPE, mBeanName);
    }

    private static String buildLabelString(Map<String, String> labels) {
        return labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining(",", ",", ""));
    }

    private static class ClientQuotaGauge extends Gauge<Double> {
        private final Quota quota;

        public ClientQuotaGauge(Quota quota) {
            this.quota = quota;
        }

        public Double value() {
            return quota.bound();
        }
    }
}
