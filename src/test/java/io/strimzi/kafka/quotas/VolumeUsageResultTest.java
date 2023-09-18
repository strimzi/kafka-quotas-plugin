/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VolumeUsageResultTest {

    @Test
    void shouldSupplyObservedAt() {
        // Given
        Instant expected = Instant.now();
        final VolumeUsage volumeUsage = new VolumeUsage("0", "/var/log/data", 1000, 1000, expected);
        VolumeUsageResult result = VolumeUsageResult.success(List.of(volumeUsage));

        // When
        Instant actual = result.getObservedAt();

        // Then
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void shouldSupplyMostRecentObservationTimestamp() {
        // Given
        Instant earlier = Instant.now().truncatedTo(ChronoUnit.DAYS);
        Instant later = earlier.plus(1, ChronoUnit.DAYS);
        final VolumeUsage volumeUsage = new VolumeUsage("0", "/var/log/data", 1000, 1000, earlier);
        final VolumeUsage volumeUsage2 = new VolumeUsage("1", "/var/log/data", 1000, 1000, later);
        VolumeUsageResult result = VolumeUsageResult.success(List.of(volumeUsage, volumeUsage2));

        // When
        Instant actual = result.getObservedAt();

        // Then
        assertThat(actual).isEqualTo(later);
    }
}
