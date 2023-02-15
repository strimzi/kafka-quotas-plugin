/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

/**
 * Provides the currently applicable throttle factor.
 */
public interface ThrottleFactorSource {
    /**
     * @return the currently applicable throttle factor.
     */
    double currentThrottleFactor();
}
