/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class CachingVolumeObserverTest {

    @Mock
    private VolumeObserver downstreamObserver;
    private VolumeObserver cachingObserver;

    @BeforeEach
    void setUp() {
        cachingObserver = new CachingVolumeObserver(downstreamObserver, Clock.systemUTC());
    }

    @Test
    void shouldPassThroughFirstObservationUnaltered() {
        // given
        VolumeUsageResult result = VolumeUsageResult.success(Set.of());

        // when
        cachingObserver.observeVolumeUsage(result);

        // then
        verify(downstreamObserver).observeVolumeUsage(result);
    }

    @Test
    void shouldMergeCachedObservationForMissingBroker() {
        // Given
        final VolumeUsage broker0Initial = new VolumeUsage("0", "/var/lib/data", 1000, 1000, Instant.now());
        final VolumeUsage broker1Initial = new VolumeUsage("1", "/var/lib/data", 1000, 1000, Instant.now());
        VolumeUsageResult primingObservation = VolumeUsageResult.success(Set.of(broker0Initial, broker1Initial));
        cachingObserver.observeVolumeUsage(primingObservation);
        final VolumeUsage broker1Final = new VolumeUsage("1", "/var/lib/data", 1000, 500, Instant.now());

        // When
        cachingObserver.observeVolumeUsage(VolumeUsageResult.success(Set.of(broker1Final)));

        // Then
        verify(downstreamObserver).observeVolumeUsage(VolumeUsageResult.success(Set.of(broker0Initial, broker1Final)));
    }

    @Test
    void shouldForwardFreshObservation() {
        // Given
        final VolumeUsage broker0Initial = new VolumeUsage("0", "/var/lib/data", 1000, 1000, Instant.now());
        final VolumeUsage broker1Initial = new VolumeUsage("1", "/var/lib/data", 1000, 1000, Instant.now());
        VolumeUsageResult primingObservation = VolumeUsageResult.success(Set.of(broker0Initial, broker1Initial));
        cachingObserver.observeVolumeUsage(primingObservation);
        final VolumeUsage broker0Final = new VolumeUsage("0", "/var/lib/data", 1000, 500, Instant.now());
        final VolumeUsage broker1Final = new VolumeUsage("1", "/var/lib/data", 1000, 500, Instant.now());
        final VolumeUsageResult expectedResult = VolumeUsageResult.success(Set.of(broker0Final, broker1Final));

        // When
        cachingObserver.observeVolumeUsage(expectedResult);

        // Then
        verify(downstreamObserver).observeVolumeUsage(expectedResult);
    }

    @Test
    void shouldForwardFailedObservation() {
        // Given
        VolumeUsageResult primingObservation = VolumeUsageResult.failure(VolumeUsageResult.VolumeSourceObservationStatus.DESCRIBE_CLUSTER_ERROR, IllegalStateException.class);

        // When
        cachingObserver.observeVolumeUsage(primingObservation);

        // Then
        verify(downstreamObserver).observeVolumeUsage(primingObservation);
    }

    @Test
    void shouldUseOldCacheEntryJustBeforeExpiry() {
        // Given
        final TickableClock clock = new TickableClock();
        cachingObserver = new CachingVolumeObserver(downstreamObserver, clock);
        final Instant initialObservationTime = clock.instant();
        final VolumeUsage broker0Initial = new VolumeUsage("0", "/var/lib/data", 1000, 1000, initialObservationTime);
        final VolumeUsage broker1Initial = new VolumeUsage("1", "/var/lib/data", 1000, 1000, initialObservationTime);
        VolumeUsageResult primingObservation = VolumeUsageResult.success(Set.of(broker0Initial, broker1Initial));
        cachingObserver.observeVolumeUsage(primingObservation);

        Duration beforeExpiry = Duration.ofSeconds(60).minusNanos(1);
        clock.tick(beforeExpiry);
        final Instant updatedObservationTime = clock.instant();
        final VolumeUsage broker1Final = new VolumeUsage("1", "/var/lib/data", 1000, 500, updatedObservationTime);
        final VolumeUsageResult subsequentObservation = VolumeUsageResult.success(Set.of(broker1Final));

        // When
        cachingObserver.observeVolumeUsage(subsequentObservation);

        // Then
        verify(downstreamObserver).observeVolumeUsage(VolumeUsageResult.success(Set.of(broker0Initial, broker1Final)));
    }

    @Test
    void shouldExpireOldCacheEntriesOnUpdateExactlyAtExpiryBoundary() {
        // Given
        final TickableClock clock = new TickableClock();
        cachingObserver = new CachingVolumeObserver(downstreamObserver, clock);
        final Instant initialObservationTime = clock.instant();
        final VolumeUsage broker0Initial = new VolumeUsage("0", "/var/lib/data", 1000, 1000, initialObservationTime);
        final VolumeUsage broker1Initial = new VolumeUsage("1", "/var/lib/data", 1000, 1000, initialObservationTime);
        VolumeUsageResult primingObservation = VolumeUsageResult.success(Set.of(broker0Initial, broker1Initial));
        cachingObserver.observeVolumeUsage(primingObservation);

        clock.tick(Duration.ofSeconds(60));
        final Instant updatedObservationTime = clock.instant();
        final VolumeUsage broker1Final = new VolumeUsage("1", "/var/lib/data", 1000, 500, updatedObservationTime);
        final VolumeUsageResult subsequentObservation = VolumeUsageResult.success(Set.of(broker1Final));

        // When
        cachingObserver.observeVolumeUsage(subsequentObservation);

        // Then
        verify(downstreamObserver).observeVolumeUsage(VolumeUsageResult.success(Set.of(broker1Final)));
    }

    @Test
    void shouldExpireOldCacheEntriesOnUpdateAfterExpiryBoundary() {
        // Given
        final TickableClock clock = new TickableClock();
        cachingObserver = new CachingVolumeObserver(downstreamObserver, clock);
        final Instant initialObservationTime = clock.instant();
        final VolumeUsage broker0Initial = new VolumeUsage("0", "/var/lib/data", 1000, 1000, initialObservationTime);
        final VolumeUsage broker1Initial = new VolumeUsage("1", "/var/lib/data", 1000, 1000, initialObservationTime);
        VolumeUsageResult primingObservation = VolumeUsageResult.success(Set.of(broker0Initial, broker1Initial));
        cachingObserver.observeVolumeUsage(primingObservation);

        Duration afterExpiryBoundary = Duration.ofSeconds(60).plusNanos(1);
        clock.tick(afterExpiryBoundary);
        final Instant updatedObservationTime = clock.instant();
        final VolumeUsage broker1Final = new VolumeUsage("1", "/var/lib/data", 1000, 500, updatedObservationTime);
        final VolumeUsageResult subsequentObservation = VolumeUsageResult.success(Set.of(broker1Final));

        // When
        cachingObserver.observeVolumeUsage(subsequentObservation);

        // Then
        verify(downstreamObserver).observeVolumeUsage(VolumeUsageResult.success(Set.of(broker1Final)));
    }
}
