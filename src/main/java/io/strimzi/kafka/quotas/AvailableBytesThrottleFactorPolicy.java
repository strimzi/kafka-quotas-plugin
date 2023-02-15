/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;

/**
 * Determines if the number of available bytes on any given volume falls below the configured limit.
 */
public class AvailableBytesThrottleFactorPolicy implements ThrottleFactorPolicy {
    private final long availableBytesLimit;

    /**
     * Creates and configures the throttle factor supplier
     *
     * @param availableBytesLimit the minimum number of bytes below which the throttle should be applied.
     */
    public AvailableBytesThrottleFactorPolicy(long availableBytesLimit) {
        this.availableBytesLimit = availableBytesLimit;
    }

    @Override
    public double calculateFactor(Collection<VolumeUsage> observedVolumes) {
        if (shouldThrottle(observedVolumes)) {
            return 0.0d;
        } else {
            return 1.0d;
        }
    }

    private boolean shouldThrottle(Collection<VolumeUsage> observedVolumes) {
        return observedVolumes.stream().anyMatch(volume -> volume.getAvailableBytes() <= availableBytesLimit);
    }
}
