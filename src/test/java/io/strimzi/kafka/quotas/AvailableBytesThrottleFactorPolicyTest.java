/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AvailableBytesThrottleFactorPolicyTest {

    private static final long AVAILABLE_BYTES_LIMIT = 100L;

    @Mock
    Runnable updateListener;

    private AvailableBytesThrottleFactorPolicy availableBytesThrottleFactorSupplier;

    @BeforeEach
    void setUp() {
        availableBytesThrottleFactorSupplier = new AvailableBytesThrottleFactorPolicy(AVAILABLE_BYTES_LIMIT);
    }

    @Test
    void shouldDefaultToUnthrottled() {
        //Given

        //When
        availableBytesThrottleFactorSupplier.addUpdateListener(updateListener);

        //Then
        verify(updateListener, times(1)).run();
        assertThrottleFactor(1.0d);
    }

    @Test
    void shouldNotifyListenerOfChangedFactor() {
        //Given
        availableBytesThrottleFactorSupplier.addUpdateListener(updateListener);
        verify(updateListener, times(1)).run();

        //When
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT)));

        //Then
        verify(updateListener, times(2)).run();
    }

    @Test
    void shouldNotNotifyListenerIfFactorUnchanged() {
        //Given
        availableBytesThrottleFactorSupplier.addUpdateListener(updateListener);
        verify(updateListener, times(1)).run();
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(900L)));

        //When
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(900L)));

        //Then
        verify(updateListener, times(1)).run();
    }

    @Test
    void shouldNotifyListenerOfFactorOnAdd() {
        //Given
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(900L)));

        //When
        availableBytesThrottleFactorSupplier.addUpdateListener(updateListener);

        //Then
        verify(updateListener).run();
    }

    @Test
    void shouldNotThrottleIfHasAvailableBytesAboveLimit() {
        //Given

        //When
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(1000L)));

        //Then
        assertThrottleFactor(1.0);
    }

    @Test
    void shouldThrottleIfHasAvailableBytesAtLimit() {
        //Given

        //When
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT)));

        //Then
        assertThrottleFactor(0.0d);
    }


    @Test
    void shouldThrottleIfHasAvailableBytesBelowLimit() {
        //Given

        //When
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT - 1)));

        //Then
        assertThrottleFactor(0.0d);
    }

    @Test
    void shouldThrottleIfAnyVolumeHasAvailableBytesBelowLimit() {
        //Given

        //When
        availableBytesThrottleFactorSupplier.observeVolumeUsage(List.of(volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT + 10),
                volumeWithAvailableBytes(AVAILABLE_BYTES_LIMIT - 1)));

        //Then
        assertThrottleFactor(0.0d);
    }

    private void assertThrottleFactor(double expected) {
        Assertions.assertThat(availableBytesThrottleFactorSupplier.get()).isCloseTo(expected, Offset.offset(0.00001));
    }

    private static VolumeUsage volumeWithAvailableBytes(long availableBytes) {
        return new VolumeUsage("0", "/var/lib/data", 1000L, availableBytes);
    }
}
