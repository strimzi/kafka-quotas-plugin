/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Determines if a fixed duration has elapsed since a given instant
 */
public class FixedDurationExpiryPolicy implements ExpiryPolicy {


    private final Duration validFor;
    private final Clock clock;

    /**
     * Create a {@link FixedDurationExpiryPolicy} with a fixed duration
     * @param clock Clock used to determine the current instant
     * @param validFor The duration during which the input is considered valid
     */
    public FixedDurationExpiryPolicy(Clock clock, Duration validFor) {
        this.clock = clock;
        this.validFor = validFor;
    }

    @Override
    public boolean isExpired(Instant validFrom) {
        return clock.instant().isAfter(validFrom.plus(validFor));
    }

}
