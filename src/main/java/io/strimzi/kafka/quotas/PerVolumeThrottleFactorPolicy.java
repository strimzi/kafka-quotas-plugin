/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Optional;

/**
 * Base Policy for limit types where we want a circuit-breaker type behaviour, throttling to a zero factor
 * if <em>any</em> volume satisfies a predicate. For example:
 * <ul>
 *     <li>Throttle to zero if any volume in the cluster is greater than 80% full</li>
 *     <li>Throttle to zero if any volume with logdir named /tmp has less than 1G available space</li>
 * </ul>
 */
abstract class PerVolumeThrottleFactorPolicy implements ThrottleFactorPolicy {

    abstract boolean shouldThrottle(VolumeUsage usage);

    @Override
    public double calculateFactor(Collection<VolumeUsage> observedVolumes) {
        Optional<VolumeUsage> anyViolation = observedVolumes.stream().filter(this::shouldThrottle).findAny();
        return anyViolation.isPresent() ? 0.0d : 1.0d;
    }

}
