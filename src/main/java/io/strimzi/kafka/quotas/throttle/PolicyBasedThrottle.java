/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import io.strimzi.kafka.quotas.VolumeObserver;
import io.strimzi.kafka.quotas.VolumeUsageObservation;
import io.strimzi.kafka.quotas.throttle.fallback.ExpiryPolicy;
import io.strimzi.kafka.quotas.throttle.fallback.FixedDurationExpiryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

import static io.strimzi.kafka.quotas.VolumeUsageObservation.VolumeSourceObservationStatus.SUCCESS;


/**
 * Converts volume usage to a throttle factor by applying a {@link ThrottleFactorPolicy} to the observations.
 * Notifies a listener when the factor changes.
 */
public class PolicyBasedThrottle implements VolumeObserver, ThrottleFactorSource {
    private static final Logger log = LoggerFactory.getLogger(PolicyBasedThrottle.class);
    private final ThrottleFactorPolicy factorPolicy;
    private final ExpiryPolicy expiryPolicy;
    private final Runnable listener;
    private final Clock clock;
    private volatile ThrottleFactor throttleFactor;

    /**
     * @param factorPolicy Which policy to apply
     * @param listener     the lister to be notified of changes
     */
    public PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener) {
        this(factorPolicy, listener, Clock.systemUTC());
    }

    PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener, Clock clock) {
        this.factorPolicy = factorPolicy;
        this.listener = listener;
        this.clock = clock;
        this.expiryPolicy = new FixedDurationExpiryPolicy(clock);
        throttleFactor = ThrottleFactor.validFactor(1.0d, clock.instant(), expiryPolicy);
    }

    @Override
    public double currentThrottleFactor() {
        return throttleFactor.getThrottleFactor();
    }

    @Override
    public void observeVolumeUsage(VolumeUsageObservation observedVolumes) {
        ThrottleFactor old = this.throttleFactor;
        this.throttleFactor = getNewFactor(observedVolumes);
        if (!Objects.equals(old.getThrottleFactor(), this.throttleFactor.getThrottleFactor())) {
            log.info("Throttle Factor changed from {} to {}, notifying listener", old, this.throttleFactor);
            listener.run();
        } else {
            log.debug("Throttle Factor unchanged at {}, not notifying listener", throttleFactor);
        }
    }

    private ThrottleFactor getNewFactor(VolumeUsageObservation observedVolumes) {
        if (observedVolumes.getStatus() == SUCCESS) {
            return calculateThrottleFactorWithExpiry(observedVolumes);
        } else {
            return maybeFallback();
        }
    }

    private ThrottleFactor calculateThrottleFactorWithExpiry(VolumeUsageObservation observedVolumes) {
        double newFactor = factorPolicy.calculateFactor(observedVolumes.getVolumeUsages());
        return ThrottleFactor.validFactor(newFactor, clock.instant(), expiryPolicy);
    }

    private ThrottleFactor maybeFallback() {
        if (throttleFactor.isExpired()) {
            return ThrottleFactor.fallbackThrottleFactor(0.0, Instant.MAX);
        } else {
            return throttleFactor;
        }
    }

}


