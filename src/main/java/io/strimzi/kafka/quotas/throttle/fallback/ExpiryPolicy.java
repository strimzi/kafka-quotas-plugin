/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle.fallback;

import java.time.Instant;

/**
 * We want to expire throttle factors if they are not updated for some time due to errors
 */
public interface ExpiryPolicy {

    ExpiryPolicy NEVER_EXPIRES = expiresAt -> false;

    boolean isExpired(Instant validFrom);
}
