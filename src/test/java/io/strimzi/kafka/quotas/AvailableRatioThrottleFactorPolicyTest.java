/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import io.strimzi.kafka.quotas.throttle.AvailableRatioThrottleFactorPolicy;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class AvailableRatioThrottleFactorPolicyTest {
    private static final Offset<Double> OFFSET = Offset.offset(0.00001d);

    private AvailableRatioThrottleFactorPolicy availableBytesThrottleFactorSupplier;

    @BeforeEach
    void setUp() {
        availableBytesThrottleFactorSupplier = new AvailableRatioThrottleFactorPolicy(0.1d);
    }

    @Test
    void shouldNotThrottleIfHasAvailableBytesAboveLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableRatio(0.11)));

        //Then
        assertThat(actualFactor).isCloseTo(1.0d, OFFSET);
    }

    @Test
    void shouldThrottleIfHasAvailableBytesAtLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableRatio(0.1)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }


    @Test
    void shouldThrottleIfHasAvailableRatioBelowLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableRatio(0.09)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }

    @Test
    void shouldThrottleIfAnyVolumeHasAvailableRatioBelowLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableRatio(0.5),
                volumeWithAvailableRatio(0.1)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }

    @Test
    void shouldThrottleIfAnyVolumeHasZeroCapacity() {
        //Given

        //When
        long capacity = 0L;
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableRatio(0.5),
                new VolumeUsage("0", "/var/lib/data", capacity, 100)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }

    private static VolumeUsage volumeWithAvailableRatio(double availableRatio) {
        int available = 100;
        return new VolumeUsage("0", "/var/lib/data", (long) (available / availableRatio), available);
    }
}
