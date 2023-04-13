/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import java.time.Instant;

/**
 * Represents a throttle factor, when it was created, its expiry policy and indicates the source:
 * 1. from a valid observation of the cluster volumes
 * 2. from fallback, a throttle factor produced because we could not successfully observe the cluster
 * <p>
 * The expiry policy determines if this ThrottleFactor has expired (is no longer valid to be applied)
 * </p>
 */
public class ThrottleFactor {

    /**
     * The source of the throttle factor:
     * 1. calculated from a valid observation
     * 2. calculated from fallback because we couldn't observe the cluster
     */
    public enum ThrottleFactorSource {
        /**
         * from a valid observation of cluster volumes
         */
        VALID_OBSERVATION,
        /**
         * from fallback
         */
        FALLBACK
    }

    private final double throttleFactor;
    private final ThrottleFactorSource source;
    private final Instant validFrom;
    private final ExpiryPolicy expiryPolicy;

    private ThrottleFactor(double throttleFactor, ThrottleFactorSource source, Instant validFrom, ExpiryPolicy policy) {
        this.throttleFactor = throttleFactor;
        this.source = source;
        this.validFrom = validFrom;
        expiryPolicy = policy;
    }

    /**
     * Provides the multiplication factor to apply to quotas.
     * @return the throttle factor
     */
    public double getThrottleFactor() {
        return throttleFactor;
    }

    /**
     * Is the current factor still valid or is it considered too old
     * @return true if expired
     */
    public boolean isExpired() {
        return expiryPolicy.isExpired(validFrom);
    }

    Instant getValidFrom() {
        return validFrom;
    }

    @Override
    public String toString() {
        return "ThrottleFactor{" +
                "throttleFactor=" + throttleFactor +
                ", source=" + source +
                ", validFrom=" + validFrom +
                '}';
    }

    /**
     * Creates a new valid factor instance
     * @param throttleFactor the factor to apply
     * @param validFrom validFrom
     * @param policy expiry policy
     * @return a throttle factor from a valid observation
     */
    public static ThrottleFactor validFactor(double throttleFactor, Instant validFrom, ExpiryPolicy policy) {
        return new ThrottleFactor(throttleFactor, ThrottleFactorSource.VALID_OBSERVATION, validFrom, policy);
    }

    /**
     * Creates a factor instance which never expires.
     * @param throttleFactor the factor to apply
     * @return a throttle factor from fallback
     */
    public static ThrottleFactor fallbackThrottleFactor(double throttleFactor) {
        return new ThrottleFactor(throttleFactor, ThrottleFactorSource.FALLBACK, Instant.now(), ExpiryPolicy.NEVER_EXPIRES);
    }
}
