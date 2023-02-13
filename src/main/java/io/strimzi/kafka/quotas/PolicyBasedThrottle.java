/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Objects;


/**
 * Converts volume usage to a throttle factor by applying a {@link ThrottleFactorPolicy} to the observations.
 * Notifies a listener when the factor changes.
 */
public class PolicyBasedThrottle implements VolumeObserver, ThrottleFactorSource {

    private final ThrottleFactorPolicy factorPolicy;
    private final Runnable listener;
    private volatile double throttleFactor = 1.0d;

    /**
     * @param factorPolicy Which policy to apply
     * @param listener the lister to be notified of changes
     */
    public PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener) {
        this.factorPolicy = factorPolicy;
        this.listener = listener;
    }

    @Override
    public double currentThrottleFactor() {
        return throttleFactor;
    }

    @Override
    public void observeVolumeUsage(Collection<VolumeUsage> observedVolumes) {
        double old = this.throttleFactor;
        this.throttleFactor = factorPolicy.calculateFactor(observedVolumes);
        if (!Objects.equals(old, this.throttleFactor)) {
            listener.run();
        }
    }
}


