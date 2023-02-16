/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import io.strimzi.kafka.quotas.VolumeUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines if the number of available bytes on any given volume falls below the configured limit.
 */
public class AvailableBytesThrottleFactorPolicy extends PerVolumeThrottleFactorPolicy {

    private static final Logger log = LoggerFactory.getLogger(AvailableBytesThrottleFactorPolicy.class);

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
    boolean shouldThrottle(VolumeUsage volume) {
        boolean shouldThrottle = volume.getAvailableBytes() <= availableBytesLimit;
        if (shouldThrottle) {
            log.debug("A volume containing logDir {} on broker {} has available bytes of {}, below the limit of {}", volume.getLogDir(), volume.getBrokerId(), volume.getAvailableBytes(), availableBytesLimit);
        }
        return shouldThrottle;
    }
}
