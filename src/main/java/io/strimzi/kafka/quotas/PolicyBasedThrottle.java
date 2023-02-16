/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static io.strimzi.kafka.quotas.VolumeUsageObservation.VolumeSourceObservationStatus.SUCCESS;


/**
 * Converts volume usage to a throttle factor by applying a {@link ThrottleFactorPolicy} to the observations.
 * Notifies a listener when the factor changes.
 */
public class PolicyBasedThrottle implements VolumeObserver, ThrottleFactorSource {
    private static final Logger log = LoggerFactory.getLogger(PolicyBasedThrottle.class);
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
    public void observeVolumeUsage(VolumeUsageObservation observedVolumes) {
        if (observedVolumes.getStatus() == SUCCESS) {
            double old = this.throttleFactor;
            this.throttleFactor = factorPolicy.calculateFactor(observedVolumes.getVolumeUsages());
            if (!Objects.equals(old, this.throttleFactor)) {
                log.info("Throttle Factor changed from {} to {}, notifying listener", old, this.throttleFactor);
                listener.run();
            } else {
                log.debug("Throttle Factor unchanged at {}, not notifying listener", throttleFactor);
            }
        }
    }
}


