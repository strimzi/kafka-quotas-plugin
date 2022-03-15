/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.strimzi.kafka.quotas.policy.CompositeFreePolicy;
import io.strimzi.kafka.quotas.policy.ConsumedSpaceQuotaPolicy;
import io.strimzi.kafka.quotas.policy.MinFreeBytesQuotaPolicy;
import io.strimzi.kafka.quotas.policy.MinFreePercentageQuotaPolicy;
import io.strimzi.kafka.quotas.policy.QuotaPolicy;
import io.strimzi.kafka.quotas.policy.UnlimitedQuotaPolicy;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.EPSILON;
import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Configuration for the static quota plugin.
 */
public class StaticQuotaConfig extends AbstractConfig {
    static final String PRODUCE_QUOTA_PROP = "client.quota.callback.static.produce";
    static final String FETCH_QUOTA_PROP = "client.quota.callback.static.fetch";
    static final String REQUEST_QUOTA_PROP = "client.quota.callback.static.request";
    static final String EXCLUDED_PRINCIPAL_NAME_LIST_PROP = "client.quota.callback.static.excluded.principal.name.list";
    static final String STORAGE_QUOTA_SOFT_PROP = "client.quota.callback.static.storage.soft";
    static final String STORAGE_QUOTA_SOFT_FREE_BYTES_PROP = "client.quota.callback.static.storage.soft.free-bytes";
    static final String STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP = "client.quota.callback.static.storage.soft.free-percent";
    static final String STORAGE_QUOTA_HARD_PROP = "client.quota.callback.static.storage.hard";
    static final String STORAGE_QUOTA_HARD_FREE_BYTES_PROP = "client.quota.callback.static.storage.hard.free-bytes";
    static final String STORAGE_QUOTA_HARD_FREE_PERCENT_PROP = "client.quota.callback.static.storage.hard.free-percent";
    static final String STORAGE_CHECK_INTERVAL_PROP = "client.quota.callback.static.storage.check-interval";
    static final String LOG_DIRS_PROP = "log.dirs";
    public static final ConfigDef.Range PERCENTAGE_RANGE = ConfigDef.Range.between(0.0, 1.0);

    private final Logger log = getLogger(StaticQuotaConfig.class);

    /**
     * Construct a configuration for the static quota plugin.
     * @param props the configuration properties
     * @param doLog whether the configurations should be logged
     */
    public StaticQuotaConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(PRODUCE_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Produce bandwidth rate quota (in bytes)")
                        .define(FETCH_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Consume bandwidth rate quota (in bytes)")
                        .define(REQUEST_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Request processing time quota (in seconds)")
                        .define(EXCLUDED_PRINCIPAL_NAME_LIST_PROP, LIST, List.of(), MEDIUM, "List of principals that are excluded from the quota")
                        .define(STORAGE_QUOTA_SOFT_PROP, LONG, Long.MAX_VALUE, LOW, "(Deprecated) Soft limit for amount of used storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_SOFT_FREE_BYTES_PROP, LONG, 0L, HIGH, "The minimum number of free bytes after which clients are throttled")
                        .define(STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP, DOUBLE, 0.0, PERCENTAGE_RANGE, HIGH, "The minimum free percentage of the volume after which clients are throttled")
                        .define(STORAGE_QUOTA_HARD_PROP, LONG, Long.MAX_VALUE, LOW, "(Deprecated) Hard limit for amount of used storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_HARD_FREE_BYTES_PROP, LONG, 0L, HIGH, "The minimum number of free bytes after which message production is effectively prevented")
                        .define(STORAGE_QUOTA_HARD_FREE_PERCENT_PROP, DOUBLE, 0.0, PERCENTAGE_RANGE, HIGH, "The minimum free percentage of the volume after which message production is effectively prevented")
                        .define(STORAGE_CHECK_INTERVAL_PROP, INT, 0, MEDIUM, "Interval between storage check runs (in seconds, default of 0 means disabled")
                        .define(LOG_DIRS_PROP, LIST, List.of(), HIGH, "Broker log directories"),
                props,
                doLog);
    }

    Map<ClientQuotaType, Quota> getQuotaMap() {
        Map<ClientQuotaType, Quota> m = new HashMap<>();
        Double produceBound = getDouble(PRODUCE_QUOTA_PROP);
        Double fetchBound = getDouble(FETCH_QUOTA_PROP);
        Double requestBound = getDouble(REQUEST_QUOTA_PROP);

        m.put(ClientQuotaType.PRODUCE, Quota.upperBound(produceBound));
        m.put(ClientQuotaType.FETCH, Quota.upperBound(fetchBound));
        m.put(ClientQuotaType.REQUEST, Quota.upperBound(requestBound));

        return m;
    }

    Long getHardStorageUsedQuota() {
        return getLong(STORAGE_QUOTA_HARD_PROP);
    }

    Long getHardFreeStorageBytes() {
        return getLong(STORAGE_QUOTA_HARD_FREE_BYTES_PROP);
    }

    Double getHardFreeStoragePercentage() {
        return getDouble(STORAGE_QUOTA_HARD_FREE_PERCENT_PROP);
    }

    Long getSoftStorageUsedQuota() {
        return getLong(STORAGE_QUOTA_SOFT_PROP);
    }

    Long getSoftFreeStorageBytes() {
        return getLong(STORAGE_QUOTA_SOFT_FREE_BYTES_PROP);
    }

    Double getSoftFreeStoragePercentage() {
        return getDouble(STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP);
    }

    int getStorageCheckInterval() {
        return getInt(STORAGE_CHECK_INTERVAL_PROP);
    }

    List<String> getLogDirs() {
        return getList(LOG_DIRS_PROP);
    }

    List<String> getExcludedPrincipalNameList() {
        return getList(EXCLUDED_PRINCIPAL_NAME_LIST_PROP);
    }

    QuotaPolicy getQuotaPolicy() {
        final Long softFreeBytes = getLong(STORAGE_QUOTA_SOFT_FREE_BYTES_PROP);
        final Long hardFreeBytes = getLong(STORAGE_QUOTA_HARD_FREE_BYTES_PROP);
        final Double softFreePercent = getDouble(STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP);
        final Double hardFreePercent = getDouble(STORAGE_QUOTA_HARD_FREE_PERCENT_PROP);
        final Long legacySoftProp = getLong(STORAGE_QUOTA_SOFT_PROP);
        final Long legacyHardProp = getLong(STORAGE_QUOTA_HARD_PROP);
        final MinFreeBytesQuotaPolicy minFreeBytesQuotaPolicy = new MinFreeBytesQuotaPolicy(softFreeBytes, hardFreeBytes);
        final MinFreePercentageQuotaPolicy minFreePercentageQuotaPolicy = new MinFreePercentageQuotaPolicy(softFreePercent, hardFreePercent);
        if (isConfigured(softFreeBytes) && isConfigured(hardFreeBytes)) {
            return minFreeBytesQuotaPolicy;
        } else if (isConfigured(softFreePercent) && isConfigured(hardFreePercent)) {
            return minFreePercentageQuotaPolicy;
        } else if (isConfigured(softFreeBytes) && isConfigured(hardFreePercent)) {
            return new CompositeFreePolicy(minFreeBytesQuotaPolicy, minFreePercentageQuotaPolicy);
        } else if (isConfigured(softFreePercent) && isConfigured(hardFreeBytes)) {
            return new CompositeFreePolicy(minFreePercentageQuotaPolicy, minFreeBytesQuotaPolicy);
        } else if (isConfigured(legacySoftProp, Long.MAX_VALUE) && isConfigured(legacyHardProp, Long.MAX_VALUE)) {
            return new ConsumedSpaceQuotaPolicy(legacySoftProp, legacyHardProp);
        } else if (isConfigured(softFreeBytes) || isConfigured(hardFreeBytes)) {
            return minFreeBytesQuotaPolicy;
        } else if (isConfigured(softFreePercent) || isConfigured(hardFreePercent)) {
            return minFreePercentageQuotaPolicy;
        }
        return UnlimitedQuotaPolicy.INSTANCE;
    }

    private boolean isConfigured(Long value) {
        return isConfigured(value, 0L);
    }

    private boolean isConfigured(Long value, long defaultValue) {
        return value != null && value != defaultValue;
    }

    private boolean isConfigured(Double value) {
        return value != null && Math.abs(value - 0.0) > EPSILON;
    }
}

