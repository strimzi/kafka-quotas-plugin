/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.throttle;

import java.time.Instant;

/**
 * An implementation of {@link ThrottleFactorSource} which applies no limits.
 */
public class UnlimitedThrottleFactorSource implements ThrottleFactorSource {

    /**
     * Global singleton instance of the Unlimited supplier.
     */
    public static final UnlimitedThrottleFactorSource UNLIMITED_THROTTLE_FACTOR_SOURCE = new UnlimitedThrottleFactorSource();

    private UnlimitedThrottleFactorSource() {
    }

    @Override
    public ThrottleFactor currentThrottleFactor() {
        return ThrottleFactor.validFactor(1.0, Instant.now(), ExpiryPolicy.NEVER_EXPIRES);
    }
}
