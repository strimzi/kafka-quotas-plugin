/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.function.Supplier;

/**
 * Determines the current restriction factor to be applied to the client quota.
 * Values returned are required to be in the range <code>0.0..1.0</code> inclusive.
 * Where a value of `1.0` implies no additional restriction over and above the defined quota.
 * A value of `0.0` implies that there is no quota available regardless of the defined quota.
 */
public interface ThrottleFactorSupplier extends Supplier<Double> {
    void addUpdateListener(Runnable listener);

}
