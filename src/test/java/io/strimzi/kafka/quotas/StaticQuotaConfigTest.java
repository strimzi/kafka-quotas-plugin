/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class StaticQuotaConfigTest {

    public static final String ARBITRARY_VALUE = "arbitrary";


    @ParameterizedTest(name = "shouldResolveListenerProperty: {0}")
    @MethodSource("org.apache.kafka.clients.admin.AdminClientConfig#configNames")
    void shouldResolveAdminConfigurations(String property) {
        //Given
        final StaticQuotaConfig.KafkaClientConfig kafkaClientConfig = new StaticQuotaConfig.KafkaClientConfig(
                Map.of("broker.id", "1", "client.quota.callback.kafka.admin." + property, ARBITRARY_VALUE),
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
                Map.of("broker.id", "1"),
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
}
