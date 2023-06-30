/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.throttle.AvailableBytesThrottleFactorPolicy;
import io.strimzi.kafka.quotas.throttle.AvailableRatioThrottleFactorPolicy;
import io.strimzi.kafka.quotas.throttle.FixedDurationExpiryPolicy;
import io.strimzi.kafka.quotas.throttle.PolicyBasedThrottle;
import io.strimzi.kafka.quotas.throttle.ThrottleFactor;
import io.strimzi.kafka.quotas.throttle.ThrottleFactorPolicy;
import io.strimzi.kafka.quotas.throttle.ThrottleFactorSource;
import io.strimzi.kafka.quotas.throttle.UnlimitedThrottleFactorSource;
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
    private volatile List<String> excludedPrincipalNameList = List.of();
    private final Set<ClientQuotaType> resetQuota = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile ThrottleFactorSource throttleFactorSource = UnlimitedThrottleFactorSource.UNLIMITED_THROTTLE_FACTOR_SOURCE;
    private final VolumeSourceBuilder volumeSourceBuilder;
    private final static String SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    private final ScheduledExecutorService backgroundScheduler;

    /**
     * Tag used for metrics to identify the broker which is recording the observation.
     */
    public static final String HOST_BROKER_TAG = "observingBrokerId";


    /**
     * Default constructor for production use.
     * <p>
     * It provides a default {@link io.strimzi.kafka.quotas.VolumeSourceBuilder#VolumeSourceBuilder()} and a scheduled executor for running background tasks on named threads.
     */
    public StaticQuotaCallback() {
        this(new VolumeSourceBuilder(), Executors.newScheduledThreadPool(2, r -> {
            final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
            thread.setDaemon(true);
            return thread;
        }));
    }

    /**
     * Secondary constructor visible for testing purposes
     *
     * @param volumeSourceBuilder the {@link io.strimzi.kafka.quotas.VolumeSourceBuilder#VolumeSourceBuilder()} to use
     * @param backgroundScheduler the scheduler for executing background tasks.
     */
    /*test*/ StaticQuotaCallback(VolumeSourceBuilder volumeSourceBuilder, ScheduledExecutorService backgroundScheduler) {
        this.volumeSourceBuilder = volumeSourceBuilder;
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
        double quota = quotaMap.getOrDefault(quotaType, Quota.upperBound(Double.MAX_VALUE)).bound();
        if (ClientQuotaType.PRODUCE.equals(quotaType)) {
            ThrottleFactor factor = throttleFactorSource.currentThrottleFactor();
            quota = quota * factor.getThrottleFactor();
        }
        // returning zero would cause a divide by zero in Kafka, so we return 1 at minimum
        return Math.max(quota, 1d);
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
            Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> SCOPE.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        StaticQuotaConfig config = new StaticQuotaConfig(configs, true);
        quotaMap = config.getQuotaMap();
        long storageCheckInterval = config.getStorageCheckInterval();
        if (storageCheckInterval > 0L) {
            LinkedHashMap<String, String> defaultTags = new LinkedHashMap<>();
            defaultTags.put(HOST_BROKER_TAG, config.getBrokerId());

            final Optional<Long> availableBytesLimitConfig = config.getAvailableBytesLimit();
            final Optional<Double> availableRatioLimit = config.getAvailableRatioLimit();
            if (availableBytesLimitConfig.isPresent() && availableRatioLimit.isPresent()) {
                String props = String.join(",", StaticQuotaConfig.AVAILABLE_BYTES_PROP, StaticQuotaConfig.AVAILABLE_RATIO_PROP);
                throw new IllegalStateException("Both limit types configured, only configure one of [" + props + "]");
            }
            ThrottleFactorPolicy throttleFactorPolicy;
            if (availableBytesLimitConfig.isPresent()) {
                throttleFactorPolicy = new AvailableBytesThrottleFactorPolicy(availableBytesLimitConfig.get());
                log.info("Available bytes limit {}", availableBytesLimitConfig.get());
            } else if (availableRatioLimit.isPresent()) {
                throttleFactorPolicy = new AvailableRatioThrottleFactorPolicy(availableRatioLimit.get());
                log.info("Available ratio limit {}", availableRatioLimit.get());
            } else {
                throw new IllegalStateException("storageCheckInterval > 0 but no limit type configured");
            }
            FixedDurationExpiryPolicy expiryPolicy = new FixedDurationExpiryPolicy(Clock.systemUTC(), config.getThrottleFactorValidityDuration());
            final PolicyBasedThrottle factorNotifier = new PolicyBasedThrottle(throttleFactorPolicy, () -> resetQuota.add(ClientQuotaType.PRODUCE), expiryPolicy, config.getFallbackThrottleFactor(), defaultTags);
            throttleFactorSource = factorNotifier;

            Runnable volumeSource = volumeSourceBuilder.withConfig(config).withVolumeObserver(factorNotifier).withDefaultTags(defaultTags).build();
            backgroundScheduler.scheduleWithFixedDelay(volumeSource, 0, storageCheckInterval, TimeUnit.SECONDS);
            backgroundScheduler.scheduleWithFixedDelay(factorNotifier::checkThrottleFactorValidity, 0, 10, TimeUnit.SECONDS);
            log.info("Configured quota callback with {}. Storage check interval: {}s", quotaMap, storageCheckInterval);
        } else {
            log.info("Static quota callback configured to never check usage: set {} to a positive value to enable", StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP);
        }
        excludedPrincipalNameList = config.getExcludedPrincipalNameList();
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

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

    static MetricName metricName(Class<?> clazz, String name, LinkedHashMap<String, String> tags) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        return metricName(name, type, group, tags);
    }

    static MetricName metricName(Class<?> clazz, String name) {
        String group = clazz.getPackageName();
        String type = clazz.getSimpleName();
        return metricName(name, type, group);
    }

    /**
     * Generate a Yammer metric name
     *
     * @param name  the name of the Metric.
     * @param type  the type to which the Metric belongs.
     * @param group the group to which the Metric belongs type
     * @return the MetricName object derived from the arguments.
     */
    public static MetricName metricName(String name, String type, String group) {
        final String sanitisedGroup = sanitise(group);
        final String sanitisedType = sanitise(type);
        final String sanitisedName = sanitise(name);
        String mBeanName = String.format("%s:type=%s,name=%s", sanitisedGroup, sanitisedType, sanitisedName);
        return new MetricName(sanitisedGroup, sanitisedType, sanitisedName, SCOPE, mBeanName);
    }

    /**
     * Generate a Yammer metric name
     *
     * @param name  the name of the Metric.
     * @param type  the type to which the Metric belongs.
     * @param group the group to which the Metric belongs type
     * @param tags  an ordered set of key value mappings
     * @return the MetricName object derived from the arguments.
     */
    public static MetricName metricName(String name, String type, String group, LinkedHashMap<String, String> tags) {
        final String tagValues = tags.entrySet().stream().map(entry -> String.format("%s=%s", sanitise(entry.getKey()), sanitise(entry.getValue()))).collect(Collectors.joining(","));
        String mBeanName;
        final String sanitisedGroup = sanitise(group);
        final String sanitisedType = sanitise(type);
        final String sanitisedName = sanitise(name);
        if (!tagValues.isBlank()) {
            mBeanName = String.format("%s:type=%s,name=%s,%s", sanitisedGroup, sanitisedType, sanitisedName, tagValues);
        } else {
            mBeanName = String.format("%s:type=%s,name=%s", sanitisedGroup, sanitisedType, sanitisedName);
        }
        return new MetricName(sanitisedGroup, sanitisedType, sanitisedName, SCOPE, mBeanName);
    }

    private static String sanitise(String name) {
        return name.replaceAll(":", "")
                .replaceAll("\\?", "")
                .replaceAll("\\*", "")
                .replaceAll("//", "")
                .replaceAll("\\$$", "");
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
