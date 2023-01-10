/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class VolumeSourceBuilderTest {

    private VolumeSourceBuilder volumeSourceBuilder;

    @Mock
    Admin adminClient;

    @BeforeEach
    void setUp() {
        volumeSourceBuilder = new VolumeSourceBuilder(VolumeSourceBuilder::testForKip827, config -> adminClient);
    }

    @AfterEach
    void tearDown() {
        if (volumeSourceBuilder != null) {
            volumeSourceBuilder.close();
        }
    }

    @Test
    void shouldReturnClusterVolumeSource() {
        //Given
        volumeSourceBuilder.withConfig(new StaticQuotaConfig(Map.of("bootstrap.servers", "localhost:9091"), false));

        //When
        final Runnable configuredRunnable = volumeSourceBuilder.build();

        //Then
        assertThat(configuredRunnable).isInstanceOf(ClusterVolumeSource.class);
    }

    @Test
    void shouldFailIfKip827NotAvailable() {
        //Given
        try (final VolumeSourceBuilder noKip827Factory = new VolumeSourceBuilder(() -> false, config -> adminClient)) {
            noKip827Factory.withConfig(new StaticQuotaConfig(Map.of(), false));
            //When
            assertThrows(IllegalStateException.class, noKip827Factory::build);
        }
        //Then
    }
}
