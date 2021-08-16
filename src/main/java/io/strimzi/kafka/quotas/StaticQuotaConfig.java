/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.server.quota.ClientQuotaType;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.DOUBLE;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class StaticQuotaConfig extends AbstractConfig {
    static final String PRODUCE_QUOTA_PROP = "client.quota.callback.static.produce";
    static final String FETCH_QUOTA_PROP = "client.quota.callback.static.fetch";
    static final String REQUEST_QUOTA_PROP = "client.quota.callback.static.request";
    static final String STORAGE_QUOTA_SOFT_PROP = "client.quota.callback.static.storage.soft";
    static final String STORAGE_QUOTA_HARD_PROP = "client.quota.callback.static.storage.hard";
    static final String STORAGE_QUOTA_PERCENTAGES_PROP = "client.quota.callback.static.storage.percentages";
    static final String STORAGE_CHECK_INTERVAL_PROP = "client.quota.callback.static.storage.check-interval";
    static final String LOG_DIRS_PROP = "log.dirs";

    public StaticQuotaConfig(Map<String, ?> props, boolean doLog) {
        super(new ConfigDef()
                        .define(PRODUCE_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Produce bandwidth rate quota (in bytes)")
                        .define(FETCH_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Consume bandwidth rate quota (in bytes)")
                        .define(REQUEST_QUOTA_PROP, DOUBLE, Double.MAX_VALUE, HIGH, "Request processing time quota (in seconds)")
                        .define(STORAGE_QUOTA_SOFT_PROP, LONG, Long.MAX_VALUE, HIGH, "Hard limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_HARD_PROP, LONG, Long.MAX_VALUE, HIGH, "Soft limit for amount of storage allowed (in bytes)")
                        .define(STORAGE_QUOTA_PERCENTAGES_PROP, STRING, "/tmp/kafka-logs;80;100", new PercentagesValidator(), HIGH, "Percentages, soft and hard, allowed for volumes (FileStore) - in format path1;soft1;hard1|path2;soft2;hard2|...")
                        .define(STORAGE_CHECK_INTERVAL_PROP, INT, 0, MEDIUM, "Interval between storage check runs (default of 0 means disabled")
                        .define(LOG_DIRS_PROP, STRING, "/tmp/kafka-logs", HIGH, "Broker log directory"),
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

    long getHardStorageQuota() {
        return getLong(STORAGE_QUOTA_HARD_PROP);
    }

    long getSoftStorageQuota() {
        return getLong(STORAGE_QUOTA_SOFT_PROP);
    }

    String getStoragePercents() {
        return getString(STORAGE_QUOTA_PERCENTAGES_PROP);
    }

    int getStorageCheckInterval() {
        return getInt(STORAGE_CHECK_INTERVAL_PROP);
    }

    String getLogDirs() {
        return getString(LOG_DIRS_PROP);
    }

    static class PercentagesValidator implements ConfigDef.Validator {
        private static final Pattern PATTERN = Pattern.compile("([^;]+;\\d+\\.?(\\d+)?;\\d+\\.?(\\d+)?)(\\|[^;]+;\\d+\\.?(\\d+)?;\\d+\\.?(\\d+)?)*");

        @Override
        public void ensureValid(String name, Object value) {
            if (!(value instanceof String)) {
                throw new InvalidConfigurationException(STORAGE_QUOTA_PERCENTAGES_PROP + " should be a string");
            }
            String string = (String) value;
            if (!PATTERN.matcher(string).matches()) {
                throw new InvalidConfigurationException(STORAGE_QUOTA_PERCENTAGES_PROP + " has invalid format: " + string);
            }
        }
    }
}

