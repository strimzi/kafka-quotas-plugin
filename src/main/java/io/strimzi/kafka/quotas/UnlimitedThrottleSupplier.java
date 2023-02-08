/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;

/**
 * An implementation of {@link ThrottleFactorPolicy} which applies no limits.
 */
public class UnlimitedThrottleSupplier implements ThrottleFactorPolicy {

    /**
     * Global singleton instance of the Unlimited supplier.
     */
    public static final UnlimitedThrottleSupplier UNLIMITED_QUOTA_SUPPLIER = new UnlimitedThrottleSupplier();

    private UnlimitedThrottleSupplier() {
    }
    @Override
    public Double currentFactor() {
        return 1.0;
    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listener.run(); //Run it once to trigger it, but otherwise it will never change.
    }

    @Override
    public void observeVolumeUsage(Collection<VolumeUsage> observedVolumes) {
        //Shrug. Update all you like I won't change my mind
    }
}
