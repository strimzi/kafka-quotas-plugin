/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle.fallback;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Produces an expiry instant a fixed duration after an input instant
 */
public class FixedDurationExpiryPolicy implements ExpiryPolicy {

    private final Duration fallbackAfter = Duration.of(5, ChronoUnit.MINUTES);
    private final Clock clock;

    public FixedDurationExpiryPolicy(Clock clock) {
        this.clock = clock;
    }


    @Override
    public boolean isExpired(Instant validFrom) {
        return clock.instant().isAfter(validFrom.plus(fallbackAfter));
    }

}
