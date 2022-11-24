/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

class StaticQuotaConfigTest {

    public static final String EXPECTED_VALUE = "/ssl/keyStore";
    public static final String ALTERNATIVE_VALUE = EXPECTED_VALUE + "/random";

    @Test
    void shouldConfigureMinimalConnectionProperties() {
        //Given
        final StaticQuotaConfig.KafkaClientConfig kafkaClientConfig = new StaticQuotaConfig.KafkaClientConfig(Map.of(), true);

        //When
        final Map<String, Object> resolvedClientConfig = kafkaClientConfig.getKafkaClientConfig();

        //Then
        assertThat(resolvedClientConfig).containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(resolvedClientConfig).hasEntrySatisfying(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, new Condition<>(o -> {
            if (o instanceof String) {
                final String s = (String) o;
                return s.endsWith(":9091");
            }
            return false;
        }, "Ends with :9091"));
        assertThat(resolvedClientConfig).containsEntry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
    }

    @ParameterizedTest(name = "shouldResolveListenerProperty: {0}")
    @MethodSource("sslProperties")
    void shouldResolveListenerProperty(String property) {
        //Given
        final StaticQuotaConfig.KafkaClientConfig kafkaClientConfig = new StaticQuotaConfig.KafkaClientConfig(
                Map.of("listener.name.replication-9091." + property, EXPECTED_VALUE,
                        "listener.name.replication-9092." + property, ALTERNATIVE_VALUE),
                true);

        //When
        final Map<String, Object> resolvedClientConfig = kafkaClientConfig.getKafkaClientConfig();

        //Then
        assertThat(resolvedClientConfig).containsEntry(property, EXPECTED_VALUE);
        assertThat(resolvedClientConfig).doesNotContainEntry(property, ALTERNATIVE_VALUE);
    }

    public static List<String> sslProperties() {
        return List.of(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
    }
}
