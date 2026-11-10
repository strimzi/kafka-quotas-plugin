/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.kafka.common.Reconfigurable;
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
 * The produce, fetch and request quotas can be changed at runtime through dynamic broker configuration.
 */
public class StaticQuotaCallback implements ClientQuotaCallback, Reconfigurable {
    /**
     * Tag used for metrics to identify the broker which generated the observation.
     */
    public static final String REMOTE_BROKER_TAG = "remoteBrokerId";
    /**
     * Tag used for metrics to identify the logDir included in the observation.
     */
    public static final String LOG_DIR_TAG = "logDir";
    private static final Logger log = LoggerFactory.getLogger(StaticQuotaCallback.class);
    private static final String EXCLUDED_PRINCIPAL_QUOTA_KEY = "excluded-principal-quota-key";
    /**
     * How long after Kafka first reads a quota change it is asked to read it once more.
     */
    static final Duration RESET_REPEAT_DELAY = Duration.ofSeconds(1);
    // States of a quota reset: nothing to report, report a change now and once more after RESET_REPEAT_DELAY, or report
    // on the next call only (the initial state). Any other value is the time, in epoch milliseconds, of the repeat.
    private static final long NO_RESET = Long.MAX_VALUE;
    private static final long RESET_REQUIRED = Long.MAX_VALUE - 1;
    private static final long RESET_ONCE = Long.MIN_VALUE;
    private final Clock clock;

    private volatile Map<ClientQuotaType, Quota> quotaMap = new HashMap<>();
    private volatile Set<String> excludedPrincipalNameList = Set.of();
    private final Map<ClientQuotaType, AtomicLong> quotaResets = new EnumMap<>(ClientQuotaType.class);
    private volatile ThrottleFactorSource throttleFactorSource = UnlimitedThrottleFactorSource.UNLIMITED_THROTTLE_FACTOR_SOURCE;
    private final VolumeSourceBuilder volumeSourceBuilder;
    private static final String SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";

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
        this(new VolumeSourceBuilder(),
                Executors.newScheduledThreadPool(2, r -> {
                    final Thread thread = new Thread(r, StaticQuotaCallback.class.getSimpleName() + "-taskExecutor");
                    thread.setDaemon(true);
                    return thread;
                }),
                Clock.systemUTC());
    }

    /**
     * Secondary constructor visible for testing purposes
     *
     * @param volumeSourceBuilder the {@link io.strimzi.kafka.quotas.VolumeSourceBuilder#VolumeSourceBuilder()} to use
     * @param backgroundScheduler the scheduler for executing background tasks.
     * @param clock the time source to use when evaluating expiry
     */
    /*test*/ StaticQuotaCallback(VolumeSourceBuilder volumeSourceBuilder,
                                 ScheduledExecutorService backgroundScheduler,
                                 Clock clock) {
        this.volumeSourceBuilder = volumeSourceBuilder;
        this.backgroundScheduler = backgroundScheduler;
        for (ClientQuotaType quotaType : ClientQuotaType.values()) {
            quotaResets.put(quotaType, new AtomicLong(RESET_ONCE));
        }
        this.clock = clock;
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
        // returning zero would cause a divide by zero in Kafka, so we return 1 at minimum. A bound that is not finite
        // (NaN, or Infinity, also times a throttle factor of 0) would stop Kafka from throttling at all, so it gets 1 too.
        if (!Double.isFinite(quota)) {
            return 1d;
        }
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
        AtomicLong reset = quotaResets.get(quotaType);
        long state = reset.get();
        if (state == NO_RESET) {
            return false;
        }
        long now = clock.millis();
        if (state == RESET_REQUIRED) {
            // Kafka does not serialise its quota refreshes: a request thread that read the old quotas can install them
            // after another thread installed the new ones. Reporting the change once more, later, narrows that window but cannot close it.
            return reset.compareAndSet(RESET_REQUIRED, now + RESET_REPEAT_DELAY.toMillis());
        }
        return now >= state && reset.compareAndSet(state, NO_RESET);
    }

    private void requireQuotaReset(ClientQuotaType quotaType) {
        quotaResets.get(quotaType).set(RESET_REQUIRED);
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
            final Optional<Long> availableBytesLimitConfig = config.getAvailableBytesLimit();
            final Optional<Double> availableRatioLimit = config.getAvailableRatioLimit();
            if (availableBytesLimitConfig.isPresent() || availableRatioLimit.isPresent()) {
                scheduleStorageCheck(config, availableBytesLimitConfig, availableRatioLimit, storageCheckInterval);
            } else {
                log.info("No volume size limits configured, storage check disabled");
            }
        } else {
            log.info("Static quota callback configured to never check usage: set {} to a positive value to enable", StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP);
        }
        excludedPrincipalNameList = config.getSetOfExcludedPrincipals();
        if (!excludedPrincipalNameList.isEmpty()) {
            log.info("Excluded principals {}", excludedPrincipalNameList);
        }

        quotaMap.keySet().forEach(clientQuotaType -> {
            String name = clientQuotaType.name().toUpperCase(ENGLISH).charAt(0) + clientQuotaType.name().toLowerCase(ENGLISH).substring(1);
            Metrics.newGauge(metricName(StaticQuotaCallback.class, name), new ClientQuotaGauge(clientQuotaType));
        });
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Set.copyOf(StaticQuotaConfig.QUOTA_PROPS.values());
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        // Only the parsing of the properties file. Kafka passes the broker's whole configuration, properties file included,
        // and every broker applies the change, so any further check could reject it on one broker only, and that broker
        // would stop applying dynamic configuration. Values that parse but make no sense get the minimum in quotaLimit().
        StaticQuotaConfig.parseQuotaMap(configs);
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        Map<ClientQuotaType, Quota> newQuotaMap = StaticQuotaConfig.parseQuotaMap(configs);
        Map<ClientQuotaType, Quota> oldQuotaMap = quotaMap;
        // Swap in a complete map: a concurrent quotaLimit() reads the old map or the new one, never a partly updated one
        quotaMap = newQuotaMap;
        // Only after the swap: a reset makes Kafka call quotaLimit() again for the clients it already tracks
        newQuotaMap.forEach((quotaType, quota) -> {
            if (Double.compare(quota.bound(), oldQuotaMap.get(quotaType).bound()) != 0) {
                requireQuotaReset(quotaType);
            }
        });
        log.info("Reconfigured quota callback with {}", newQuotaMap);
    }

    private void scheduleStorageCheck(StaticQuotaConfig config, Optional<Long> availableBytesLimitConfig, Optional<Double> availableRatioLimit, long storageCheckInterval) {
        LinkedHashMap<String, String> defaultTags = new LinkedHashMap<>();
        defaultTags.put(HOST_BROKER_TAG, config.getBrokerId());
        if (availableBytesLimitConfig.isPresent() && availableRatioLimit.isPresent()) {
            String props = String.join(",", StaticQuotaConfig.AVAILABLE_BYTES_PROP, StaticQuotaConfig.AVAILABLE_RATIO_PROP);
            throw new IllegalStateException("Both limit types configured, only configure one of [" + props + "]");
        }
        ThrottleFactorPolicy throttleFactorPolicy;
        if (availableBytesLimitConfig.isPresent()) {
            throttleFactorPolicy = new AvailableBytesThrottleFactorPolicy(availableBytesLimitConfig.get());
            log.info("Available bytes limit {}", availableBytesLimitConfig.get());
        } else {
            throttleFactorPolicy = new AvailableRatioThrottleFactorPolicy(availableRatioLimit.get());
            log.info("Available ratio limit {}", availableRatioLimit.get());
        }
        FixedDurationExpiryPolicy expiryPolicy = new FixedDurationExpiryPolicy(clock, config.getThrottleFactorValidityDuration());
        final PolicyBasedThrottle factorNotifier = new PolicyBasedThrottle(throttleFactorPolicy, () -> requireQuotaReset(ClientQuotaType.PRODUCE), expiryPolicy, config.getFallbackThrottleFactor(), defaultTags);
        throttleFactorSource = factorNotifier;
        VolumeObserver observer = new CachingVolumeObserver(factorNotifier, clock, config.getThrottleFactorValidityDuration(), defaultTags);
        Runnable volumeSource = volumeSourceBuilder.withConfig(config).withVolumeObserver(observer).withDefaultTags(defaultTags).build();
        backgroundScheduler.scheduleWithFixedDelay(volumeSource, 0, storageCheckInterval, TimeUnit.SECONDS);
        backgroundScheduler.scheduleWithFixedDelay(factorNotifier::checkThrottleFactorValidity, 0, 10, TimeUnit.SECONDS);
        log.info("Configured quota callback with {}. Storage check interval: {}s", quotaMap, storageCheckInterval);
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

    /**
     * MetricNames are translated to mbean names which need to comply with the @link{<a href="https://docs.oracle.com/en/java/javase/17/docs/api/java.management/javax/management/ObjectName.html">javax.management.ObjectName</a>} rules.
     * Unfortunately the constructors we use from Yammer don't validate the metric names, so we do it ourselves.
     *
     * @param name value to be sanitised
     * @return the value with all illegal characters removed
     */
    private static String sanitise(String name) {
        // It would be more efficient to create a single pattern and replace everything once.
        // However, I think this makes things clearer and easier to understand.
        return name.replaceAll("[:?*=,]", "")
                .replaceAll("//", "") // Double slashes are reserved as a protocol specifier
                .replaceAll("\\$$", ""); // $ is only illegal as a trailing character as it is used to denote inner classes.
    }

    private class ClientQuotaGauge extends Gauge<Double> {
        private final ClientQuotaType quotaType;

        public ClientQuotaGauge(ClientQuotaType quotaType) {
            this.quotaType = quotaType;
        }

        public Double value() {
            return quotaMap.get(quotaType).bound();
        }
    }
}
