/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.slf4j.Logger;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LIST;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Configuration for the static quota plugin.
 */
public class StaticQuotaConfig extends AbstractConfig {
    private static final String CLIENT_QUOTA_CALLBACK_STATIC_PREFIX = "client.quota.callback.static";
    static final String PRODUCE_QUOTA_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".produce";
    static final String FETCH_QUOTA_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".fetch";
    static final String REQUEST_QUOTA_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".request";
    static final String EXCLUDED_PRINCIPAL_NAME_LIST_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".excluded.principal.name.list";
    static final String STORAGE_QUOTA_SOFT_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".storage.soft";
    static final String STORAGE_QUOTA_HARD_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".storage.hard";
    static final String STORAGE_CHECK_INTERVAL_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".storage.check-interval";
    static final String LOG_DIRS_PROP = "log.dirs";
    public static final String AVAILABLE_BYTES_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".storage.per.volume.limit.min.available.bytes";
    public static final String ADMIN_BOOTSTRAP_SERVER_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".kafka.admin.bootstrap.servers";
    private final KafkaClientConfig kafkaClientConfig;

    /**
     * Construct a configuration for the static quota plugin.
     *
     * @param props the configuration properties
     * @param doLog whether the configurations should be logged
     */
    public StaticQuotaConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(PRODUCE_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Produce bandwidth rate quota (in bytes)")
                        .define(FETCH_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Consume bandwidth rate quota (in bytes)")
                        .define(REQUEST_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Request processing time quota (in seconds)")
                        .define(EXCLUDED_PRINCIPAL_NAME_LIST_PROP, LIST, List.of(), MEDIUM, "List of principals that are excluded from the quota")
                        .define(STORAGE_QUOTA_SOFT_PROP, LONG, Long.MAX_VALUE, HIGH, "Hard limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_HARD_PROP, LONG, Long.MAX_VALUE, HIGH, "Soft limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_CHECK_INTERVAL_PROP, INT, 0, MEDIUM, "Interval between storage check runs (in seconds, default of 0 means disabled")
                        .define(AVAILABLE_BYTES_PROP, LONG, null, nullOrAtLeastValidator(0), MEDIUM, "stop message production if availableBytes <= this value")
                        .define(LOG_DIRS_PROP, LIST, List.of(), HIGH, "Broker log directories"),
                props,
                doLog);
        kafkaClientConfig = new KafkaClientConfig(props, doLog);
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

    long getHardStorageQuota() {
        return getLong(STORAGE_QUOTA_HARD_PROP);
    }

    long getSoftStorageQuota() {
        return getLong(STORAGE_QUOTA_SOFT_PROP);
    }

    Optional<Long> getAvailableBytesLimit() {
        //TODO range validation.
        return Optional.ofNullable(getLong(AVAILABLE_BYTES_PROP));
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

    KafkaClientConfig getKafkaClientConfig() {
        return kafkaClientConfig;
    }

    private static ConfigDef.LambdaValidator nullOrAtLeastValidator(int min) {
        ConfigDef.Range atLeast = ConfigDef.Range.atLeast(min);
        return ConfigDef.LambdaValidator.with((name, value) -> {
            if (value != null) {
                atLeast.ensureValid(name, value);
            }
        }, atLeast::toString);
    }

    static class KafkaClientConfig extends AbstractConfig {
        public static final String CLIENT_ID_PREFIX_PROP = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".kafka.clientIdPrefix";
        public static final String ADMIN_CONFIG_PREFIX = CLIENT_QUOTA_CALLBACK_STATIC_PREFIX + ".kafka.admin.";
        private final Logger log = getLogger(KafkaClientConfig.class);

        @SuppressWarnings("unchecked")
        public KafkaClientConfig(Map<String, ?> props, boolean doLog) {
            super(new ConfigDef()
                            .define(CLIENT_ID_PREFIX_PROP, STRING, "__strimzi", LOW, "Prefix to use when creating client.ids")
                            .define(ADMIN_BOOTSTRAP_SERVER_PROP, LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.LambdaValidator.with((name, value) -> {
                                if (value instanceof List) {
                                    final List<String> configuredValue = (List<String>) value;
                                    if (configuredValue.isEmpty()) {
                                        throw new ConfigException(name, value, "Value was an empty list");
                                    }
                                } else {
                                    throw new ConfigException(name, value, "Value was not a List");
                                }
                            }, () -> "Invalid bootstrap servers provided."), HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC),
                    props,
                    doLog);
        }

        public Map<String, Object> getKafkaClientConfig() {
            Map<String, Object> configuredProperties = originalsWithPrefix(ADMIN_CONFIG_PREFIX, true);
            configuredProperties.computeIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, this::generateClientId);
            log.info("resolved kafka config of {}", configuredProperties);
            return configuredProperties;
        }

        private Object generateClientId(String key) {
            return get(CLIENT_ID_PREFIX_PROP) + "-" + originals().get("broker.id") + "-" + UUID.randomUUID();
        }
    }

}

