/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Determines if the number of available bytes on any given volume falls below the configured limit.
 */
public class AvailableBytesThrottleFactorSupplier implements ThrottleFactorSupplier {
    private final List<Runnable> listeners;
    private final long availableBytesLimit;

    private volatile boolean throttled = false;

    /**
     * Creates and configures the throttle factor supplier
     * @param availableBytesLimit the minimum number of bytes below which the throttle should be applied.
     */
    public AvailableBytesThrottleFactorSupplier(long availableBytesLimit) {
        this.availableBytesLimit = availableBytesLimit;
        listeners = new CopyOnWriteArrayList<>();
    }

    /**
     * Accepts an updated collection of volumes to validate against the configured limit.
     * @param volumes the new collection of volumes to be considered
     */
    @Override
    public void accept(Collection<Volume> volumes) {
        boolean initial = throttled;
        throttled = calculateNewFactor(volumes);
        if (throttled != initial) {
            listeners.forEach(Runnable::run);
        }
    }

    private boolean calculateNewFactor(Collection<Volume> volumes) {
        return volumes.stream().anyMatch(volume -> volume.getAvailableBytes() <= availableBytesLimit);
    }

    @Override
    public Double get() {
        return throttled ? 0.0 : 1.0;
    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listeners.add(listener);
        listener.run();
    }
}
