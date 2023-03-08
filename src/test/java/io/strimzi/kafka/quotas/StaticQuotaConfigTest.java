/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.strimzi.kafka.quotas.StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class StaticQuotaConfigTest {

    public static final String ARBITRARY_VALUE = "arbitrary";
    private Map<String, String> defaultProps = Map.of("broker.id", "1",
            "client.quota.callback.static.kafka.admin.bootstrap.servers", "localhost:9093");

    @ParameterizedTest(name = "shouldResolveListenerProperty: {0}")
    @MethodSource("org.apache.kafka.clients.admin.AdminClientConfig#configNames")
    void shouldResolveAdminConfigurations(String property) {
        //Given
        final Map<String, String> props = new HashMap<>(defaultProps);
        props.put("client.quota.callback.static.kafka.admin." + property, ARBITRARY_VALUE); //replace client.quota.callback.static.kafka.admin.bootstrap.servers
        final StaticQuotaConfig.KafkaClientConfig kafkaClientConfig = new StaticQuotaConfig.KafkaClientConfig(
                props,
                true);

        //When
        final Map<String, Object> resolvedClientConfig = kafkaClientConfig.getKafkaClientConfig();

        //Then
        assertThat(resolvedClientConfig).containsEntry(property, ARBITRARY_VALUE);
    }

    @Test
    void generateDefaultClientIdWithPrefixIfNoneConfigured() {
        //Given
        final StaticQuotaConfig.KafkaClientConfig kafkaClientConfig = new StaticQuotaConfig.KafkaClientConfig(
                defaultProps,
                true);

        //When
        final Map<String, Object> resolvedClientConfig = kafkaClientConfig.getKafkaClientConfig();

        //Then
        assertThat(resolvedClientConfig).hasEntrySatisfying(AdminClientConfig.CLIENT_ID_CONFIG, o -> {
            if (o instanceof String) {
                assertThat((String) o).startsWith("__strimzi-1-");
            } else {
                fail(AdminClientConfig.CLIENT_ID_CONFIG + " was not a string");
            }
        });
    }

    @Test
    void negativeAvailableBytesNotAllowed() {
        //When
        assertThatThrownBy(() -> new StaticQuotaConfig(Map.of(StaticQuotaConfig.AVAILABLE_BYTES_PROP, "-1"), true))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value -1");
    }

    @Test
    void invalidThrottleValidityDurationNotAllowed() {
        //When
        assertThatThrownBy(() -> new StaticQuotaConfig(Map.of(StaticQuotaConfig.THROTTLE_FALLBACK_VALIDITY_DURATION, "NOT 8601 FRIENDLY"), true))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value NOT 8601 FRIENDLY");
    }

    @Test
    void validThrottleValidityDurationAllowed() {
        //Given
        HashMap<String, String> props = new HashMap<>(defaultProps);
        props.put(StaticQuotaConfig.THROTTLE_FALLBACK_VALIDITY_DURATION, "PT10M");

        //When
        StaticQuotaConfig config = new StaticQuotaConfig(props, true);

        //Then
        assertThat(config.getThrottleFactorValidityDuration()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void validFallbackThrottleFactorAllowed() {

        testValidFallbackThrottleFactor("0.5", 0.5d);
    }

    @Test
    void validFallbackThrottleFactorZeroAllowed() {
        //Given
        testValidFallbackThrottleFactor("0", 0d);
    }

    @Test
    void validFallbackThrottleFactorOneAllowed() {
        //Given
        testValidFallbackThrottleFactor("1", 1d);
    }

    private void testValidFallbackThrottleFactor(String property, double expected) {
        //Given
        HashMap<String, String> props = new HashMap<>(defaultProps);
        props.put(StaticQuotaConfig.FALLBACK_THROTTLE_FACTOR, property);

        //When
        StaticQuotaConfig config = new StaticQuotaConfig(props, true);

        //Then
        assertThat(config.getFallbackThrottleFactor()).isEqualTo(expected);
    }

    @Test
    void negativeFallbackThrottleFactorNotAllowed() {
        testInvalidFallbackThrottleFactor("-0.001");
    }

    @Test
    void fallbackThrottleFactorOverOneNotAllowed() {
        testInvalidFallbackThrottleFactor("1.001");
    }

    private static void testInvalidFallbackThrottleFactor(String prop) {
        //When
        assertThatThrownBy(() -> new StaticQuotaConfig(Map.of(StaticQuotaConfig.FALLBACK_THROTTLE_FACTOR, prop), true))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Invalid value " + prop);
    }

    @Test
    void testDefaultValues() {
        //When
        StaticQuotaConfig config = new StaticQuotaConfig(defaultProps, true);

        //Then
        assertThat(config.getThrottleFactorValidityDuration()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.getFallbackThrottleFactor()).isEqualTo(1.0d);
    }

    @Test
    void adminBootstrapServersRequired() {
        //When
        assertThatThrownBy(() -> new StaticQuotaConfig(Map.of(), true))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("Missing required configuration \"" + ADMIN_BOOTSTRAP_SERVER_PROP + "\" which has no default value.");
    }

}
