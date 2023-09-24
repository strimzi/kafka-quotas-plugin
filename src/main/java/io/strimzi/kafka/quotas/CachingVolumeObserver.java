/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static io.strimzi.kafka.quotas.VolumeUsageResult.VolumeSourceObservationStatus.SUCCESS;

/**
 * Volume Observer that remembers the most recent observation for each brokerId. Then when it observes new result it
 * can augment in observations from brokers that have dropped out of the results (due to being restarted or otherwise
 * removed from the active set of brokers).
 */
public class CachingVolumeObserver implements VolumeObserver {
    private final VolumeObserver observer;
    private final Clock clock;
    private final Duration entriesValidFor;
    private final ConcurrentMap<CacheKey, VolumeUsage> cachedObservations;

    /**
     * @param observer The downstream observer to be notified after processing
     * @param clock the clock to use for managing cache expiry
     * @param entriesValidFor how long cache valid entries for
     */
    public CachingVolumeObserver(VolumeObserver observer, Clock clock, Duration entriesValidFor) {
        this.observer = observer;
        this.clock = clock;
        this.entriesValidFor = entriesValidFor;
        cachedObservations = new ConcurrentHashMap<>();
    }

    @Override
    public void observeVolumeUsage(VolumeUsageResult result) {
        maybeExpireCachedObservations();
        VolumeUsageResult outgoing;
        if (result.getStatus() == SUCCESS) {
            outgoing = cacheAndAugment(result);
        } else {
            outgoing = result;
        }
        observer.observeVolumeUsage(outgoing);
    }

    private void maybeExpireCachedObservations() {
        Set<CacheKey> toRemove = cachedObservations.entrySet().stream()
                .filter(e -> isExpired(e.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
        toRemove.forEach(cachedObservations::remove);
    }

    private boolean isExpired(VolumeUsage usage) {
        Instant expiry = usage.getObservedAt().plus(entriesValidFor);
        Instant now = clock.instant();
        return now.equals(expiry) || now.isAfter(expiry);
    }

    private VolumeUsageResult cacheAndAugment(VolumeUsageResult result) {
        cachedObservations.putAll(result.getVolumeUsages()
                .stream()
                .collect(Collectors.toMap(usage -> new CacheKey(usage.getBrokerId(), usage.getLogDir()), usage -> usage)));

        final Collection<VolumeUsage> mergedUsage = Set.copyOf(cachedObservations.values());

        return VolumeUsageResult.replaceObservations(result, mergedUsage);
    }

    private static class CacheKey {

        private final String brokerId;
        private final String logDir;

        public CacheKey(String topic, String logDir) {
            this.brokerId = topic;
            this.logDir = logDir;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(brokerId, cacheKey.brokerId) && Objects.equals(logDir, cacheKey.logDir);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerId, logDir);
        }
    }
}
