/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import org.slf4j.Logger;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.metricName;
import static io.strimzi.kafka.quotas.VolumeUsageResult.VolumeSourceObservationStatus.SUCCESS;
import static org.slf4j.LoggerFactory.getLogger;

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
    private final LinkedHashMap<String, String> defaultTags;
    private final Map<CacheKey, Counter> evictionsPerRemoteBroker = new ConcurrentHashMap<>();

    private final Logger log = getLogger(CachingVolumeObserver.class);

    /**
     * @param observer The downstream observer to be notified after processing
     * @param clock the clock to use for managing cache expiry
     * @param entriesValidFor how long cache valid entries for
     * @param defaultTags tags to be added to all metrics
     */
    public CachingVolumeObserver(VolumeObserver observer, Clock clock, Duration entriesValidFor, LinkedHashMap<String, String> defaultTags) {
        this.observer = observer;
        this.clock = clock;
        this.entriesValidFor = entriesValidFor;
        this.defaultTags = new LinkedHashMap<>(defaultTags);
        cachedObservations = new ConcurrentHashMap<>();
        Metrics.newGauge(metricName(CachingVolumeObserver.class, "CachedEntries", defaultTags), new Gauge<>() {
            @Override
            public Integer value() {
                return cachedObservations.size();
            }
        });
    }

    @Override
    public void observeVolumeUsage(VolumeUsageResult result) {
        VolumeUsageResult outgoing;
        if (result.getStatus() == SUCCESS) {
            if (log.isDebugEnabled()) {
                log.debug("Caching successful observation. Propagating observation along with cached values where appropriate to the next observer.");
            }
            outgoing = cacheAndAugment(result);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Not caching failed observation. Propagating failed observation to the next observer.");
            }
            maybeExpireCachedObservations();
            outgoing = result;
        }
        observer.observeVolumeUsage(outgoing);
    }

    private void maybeExpireCachedObservations() {
        Set<CacheKey> toRemove = cachedObservations.entrySet().stream()
                .filter(e -> isExpired(e.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
        toRemove.forEach(this::evict);
    }

    private void evict(CacheKey key) {
        final Counter counter = getEvictionCounter(key);
        counter.inc();
        if (log.isDebugEnabled()) {
            log.debug("evicting entry logDir: {} on broker: {}", key.logDir, key.brokerId);
        }
        cachedObservations.remove(key);
    }

    private Counter getEvictionCounter(CacheKey key) {
        return evictionsPerRemoteBroker.computeIfAbsent(key, key1 -> buildCounterForCacheKey("LogDirEvictions", key1));
    }

    private Counter buildCounterForCacheKey(String name, CacheKey cacheKey) {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>(defaultTags);
        tags.put(StaticQuotaCallback.REMOTE_BROKER_TAG, cacheKey.brokerId);
        tags.put(StaticQuotaCallback.LOG_DIR_TAG, cacheKey.logDir);
        return Metrics.newCounter(metricName(CachingVolumeObserver.class, name, tags));
    }

    private boolean isExpired(VolumeUsage usage) {
        Instant expiry = usage.getObservedAt().plus(entriesValidFor);
        Instant now = clock.instant();
        return now.equals(expiry) || now.isAfter(expiry);
    }

    private VolumeUsageResult cacheAndAugment(VolumeUsageResult result) {
        cachedObservations.putAll(result.getVolumeUsages()
                .stream()
                .collect(Collectors.toMap(this::createCacheKey, usage -> usage)));

        maybeExpireCachedObservations();
        final Collection<VolumeUsage> mergedUsage = Set.copyOf(cachedObservations.values());

        return VolumeUsageResult.replaceObservations(result, mergedUsage);
    }

    private CacheKey createCacheKey(VolumeUsage usage) {
        final CacheKey key = new CacheKey(usage.getBrokerId(), usage.getLogDir());
        getEvictionCounter(key);
        return key;
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
