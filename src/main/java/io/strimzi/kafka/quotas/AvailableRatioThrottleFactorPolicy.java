/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Determines if the available ratio on any given volume falls below the configured limit.
 */
public class AvailableRatioThrottleFactorPolicy extends PerVolumeThrottleFactorPolicy {

    private static final Logger log = LoggerFactory.getLogger(AvailableRatioThrottleFactorPolicy.class);

    private final double availableBytesRatio;

    /**
     * Creates and configures the throttle factor supplier
     *
     * @param availableBytesRatio the minimum volume available ratio below which the throttle should be applied.
     */
    public AvailableRatioThrottleFactorPolicy(double availableBytesRatio) {
        this.availableBytesRatio = availableBytesRatio;
    }

    @Override
    boolean shouldThrottle(VolumeUsage volume) {
        boolean shouldThrottle = volume.getAvailableRatio() <= availableBytesRatio;
        if (shouldThrottle) {
            log.debug("A volume containing logDir {} on broker {} has available ratio of {}, below the limit of {}", volume.getLogDir(), volume.getBrokerId(), volume.getAvailableBytes(), availableBytesRatio);
        }
        return shouldThrottle;
    }
}
