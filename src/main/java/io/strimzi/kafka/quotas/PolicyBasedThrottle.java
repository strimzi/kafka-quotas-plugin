/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.Objects;


/**
 *
 */
public class PolicyBasedThrottle implements VolumeObserver, ThrottleFactorSource {

    private final ThrottleFactorPolicy factorPolicy;
    private final Runnable listener;
    private volatile Double throttleFactor = 1.0d;

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
        Double old = this.throttleFactor;
        this.throttleFactor = factorPolicy.calculateFactor(observedVolumes);
        if (!Objects.equals(old, this.throttleFactor)) {
            listener.run();
        }
    }
}


