/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Determines if the number of available bytes on any given volume falls below the configured limit.
 */
public class AvailableBytesThrottleFactorPolicy implements ThrottleFactorPolicy {
    private final Logger log = getLogger(AvailableBytesThrottleFactorPolicy.class);
    private final List<Runnable> listeners;
    private final long availableBytesLimit;

    private volatile boolean throttled = false;

    /**
     * Creates and configures the throttle factor supplier
     * @param availableBytesLimit the minimum number of bytes below which the throttle should be applied.
     */
    public AvailableBytesThrottleFactorPolicy(long availableBytesLimit) {
        this.availableBytesLimit = availableBytesLimit;
        listeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Accepts an updated collection of volumes to validate against the configured limit.
     * @param volumes the new collection of volumes to be considered
     */
    @Override
    public void observeVolumeUsage(Collection<VolumeUsage> volumes) {
        boolean initial = throttled;
        throttled = shouldThrottle(volumes);
        if (throttled != initial) {
            log.debug("throttle status changed from: {} to {} on update", initial, throttled);
            listeners.forEach(Runnable::run);
        }
    }

    private boolean shouldThrottle(Collection<VolumeUsage> volumes) {
        return volumes.stream().anyMatch(volume -> volume.getAvailableBytes() <= availableBytesLimit);
    }

    @Override
    public double currentFactor() {
        return throttled ? 0.0 : 1.0;
    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listeners.add(listener);
        listener.run();
    }
}
