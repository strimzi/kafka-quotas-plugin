/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import io.strimzi.kafka.quotas.VolumeUsage;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.metricName;

/**
 * Base Policy for limit types where we want a circuit-breaker type behaviour, throttling to a zero factor
 * if <em>any</em> volume satisfies a predicate. For example:
 * <ul>
 *     <li>Throttle to zero if any volume in the cluster is greater than 80% full</li>
 *     <li>Throttle to zero if any volume with logdir named /tmp has less than 1G available space</li>
 * </ul>
 */
abstract class PerVolumeThrottleFactorPolicy implements ThrottleFactorPolicy {

    private final Counter limitViolationCounter;

    public PerVolumeThrottleFactorPolicy() {
        limitViolationCounter = Metrics.newCounter(metricName("LimitViolated", "ThrottleFactor", "io.strimzi.kafka.quotas"));
    }

    abstract boolean shouldThrottle(VolumeUsage usage);

    @Override
    public double calculateFactor(Collection<VolumeUsage> observedVolumes) {
        Set<VolumeUsage> violations = observedVolumes.stream().filter(this::shouldThrottle).collect(Collectors.toUnmodifiableSet());
        limitViolationCounter.inc(violations.size());
        return violations.isEmpty() ? 1.0d : 0.0d;
    }

}
