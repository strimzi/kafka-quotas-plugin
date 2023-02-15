/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.List;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class AvailableBytesThrottleFactorPolicyTest {

    private static final long AVAILABLE_BYTES_LIMIT = 100L;
    private static final Offset<Double> OFFSET = Offset.offset(0.00001d);

    private AvailableBytesThrottleFactorPolicy availableBytesThrottleFactorSupplier;

    @BeforeEach
    void setUp() {
        availableBytesThrottleFactorSupplier = new AvailableBytesThrottleFactorPolicy(AVAILABLE_BYTES_LIMIT);
    }

    @Test
    void shouldNotThrottleIfHasAvailableBytesAboveLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableBytes(1000L)));

        //Then
        assertThat(actualFactor).isCloseTo(1.0d, OFFSET);
    }

    @Test
    void shouldThrottleIfHasAvailableBytesAtLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }


    @Test
    void shouldThrottleIfHasAvailableBytesBelowLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT - 1)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }

    @Test
    void shouldThrottleIfAnyVolumeHasAvailableBytesBelowLimit() {
        //Given

        //When
        final double actualFactor = availableBytesThrottleFactorSupplier.calculateFactor(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT + 10),
                volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT - 1)));

        //Then
        assertThat(actualFactor).isCloseTo(0.0d, OFFSET);
    }

    private static VolumeUsage volumeWithAvailableBytes(long availableBytes) {
        return new VolumeUsage("0", "/var/lib/data", 1000L, availableBytes);
    }
}
