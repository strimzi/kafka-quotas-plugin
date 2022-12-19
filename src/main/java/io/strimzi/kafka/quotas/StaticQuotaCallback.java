/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

import static java.util.Locale.ENGLISH;

/**
 * Allows configuring generic quotas for a broker independent of users and clients.
 */
public class StaticQuotaCallback implements ClientQuotaCallback {
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";

    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private volatile long storageQuotaSoft = Long.MAX_VALUE;
    private volatile long storageQuotaHard = Long.MAX_VALUE;
    private volatile List<String> excludedPrincipalNameList = List.of();
    private final Set<ClientQuotaType> resetQuota = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile ThrottleFactorSupplier throttleFactorSupplier = UnlimitedThrottleSupplier.UNLIMITED_QUOTA_SUPPLIER;
    private final VolumeSourceBuilder volumeSourceBuilder;
    private final String scope = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    private final ScheduledExecutorService backgroundScheduler;

    /**
     * Constructs the Static Quota Callback class
     */
    public StaticQuotaCallback() {
        this(new VolumeSourceBuilder(), Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
            thread.setDaemon(true);
            return thread;
        }));
    }

    /*test*/ StaticQuotaCallback(VolumeSourceBuilder localVolumeSource, ScheduledExecutorService backgroundScheduler) {
        this.volumeSourceBuilder = localVolumeSource;
        this.backgroundScheduler = backgroundScheduler;
        Collections.addAll(resetQuota, ClientQuotaType.values());
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
        double quota = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();

        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            return quota * throttleFactorSupplier.get();
        } else {
            return quota;
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
        return resetQuota.remove(quotaType);
    }

    @Override
    public boolean updateClusterMetadata(Cluster cluster) {
        return false;
    }

    @Override
    public void close() {
        try {
            closeExecutorService();
            volumeSourceBuilder.close();
        } finally {
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> scope.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        storageQuotaSoft = config.getSoftStorageQuota();
        storageQuotaHard = config.getHardStorageQuota();
        throttleFactorSupplier = new TotalConsumedThrottleFactorSupplier(storageQuotaHard, storageQuotaSoft);
        throttleFactorSupplier.addUpdateListener(() -> resetQuota.add(ClientQuotaType.PRODUCE));
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();

        long storageCheckIntervalMillis = TimeUnit.SECONDS.toMillis(config.getStorageCheckInterval());

        if (storageCheckIntervalMillis > 0L) {
            Runnable job = volumeSourceBuilder.withConfig(config).withVolumeConsumer(this::updateVolumes).build();
            backgroundScheduler.scheduleWithFixedDelay(job, storageCheckIntervalMillis, storageCheckIntervalMillis, TimeUnit.MILLISECONDS);
            log.info("Configured quota callback with {}. Storage quota (soft, hard): ({}, {}). Storage check interval: {}ms", quotaMap, storageQuotaSoft, storageQuotaHard, storageCheckIntervalMillis);
        }
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        Metrics.newGauge(metricName(StorageChecker.class, "TotalStorageUsedBytes"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "SoftLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaSoft;
            }
        });
        Metrics.newGauge(metricName(StorageChecker.class, "HardLimitBytes"), new Gauge<Long>() {
            public Long value() {
                return storageQuotaHard;
            }
        });

        quotaMap.forEach((clientQuotaType, quota) -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(quota));
        });
    }

    private void closeExecutorService() {
        try {
            backgroundScheduler.shutdownNow();
        } catch (Exception e) {
            log.warn("Encountered problem shutting down background executor: {}", e.getMessage(), e);
        }
    }

    private MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        String mBeanName = String.format("%s:type=%s,name=%s", group, type, name);
        return new MetricName(group, type, name, this.scope, mBeanName);
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
