/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle.fallback;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

/**
 * Produces an expiry instant a fixed duration after an input instant
 */
public class FixedDurationExpiryPolicy implements ExpiryPolicy {


    private final Duration expireAfter;
    private final Clock clock;

    /**
     * Create a {@link FixedDurationExpiryPolicy} with a fixed duration
     * @param clock Clock used to determine the current instant
     * @param expireAfter Duration which input is expired after
     */
    public FixedDurationExpiryPolicy(Clock clock, Duration expireAfter) {
        this.clock = clock;
        this.expireAfter = expireAfter;
    }

    @Override
    public boolean isExpired(Instant validFrom) {
        return clock.instant().isAfter(validFrom.plus(expireAfter));
    }

}
