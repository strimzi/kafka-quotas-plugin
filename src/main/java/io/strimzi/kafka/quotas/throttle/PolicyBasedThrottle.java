/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import io.strimzi.kafka.quotas.VolumeObserver;
import io.strimzi.kafka.quotas.VolumeUsageObservation;
import io.strimzi.kafka.quotas.throttle.fallback.ExpiryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Objects;
import java.util.function.Function;

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
    private final double fallbackThrottleFactor;

    /**
     * @param factorPolicy Which policy to apply
     * @param listener     the lister to be notified of changes
     * @param expiryPolicy expiry policy to control how long a factor is applied for
     * @param fallbackThrottleFactor throttle factor to apply if a factor reaches its expiry
     */
    public PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener, ExpiryPolicy expiryPolicy, double fallbackThrottleFactor) {
        this(factorPolicy, listener, Clock.systemUTC(), expiryPolicy, fallbackThrottleFactor);
    }

    PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener, Clock clock, ExpiryPolicy expiryPolicy, double fallbackThrottleFactor) {
        this.factorPolicy = factorPolicy;
        this.listener = listener;
        this.clock = clock;
        this.expiryPolicy = expiryPolicy;
        throttleFactor = ThrottleFactor.validFactor(1.0d, clock.instant(), this.expiryPolicy);
        this.fallbackThrottleFactor = fallbackThrottleFactor;
    }

    @Override
    public ThrottleFactor currentThrottleFactor() {
        return throttleFactor;
    }

    @Override
    public void observeVolumeUsage(VolumeUsageObservation observedVolumes) {
        updateFactor(current -> getNewFactor(observedVolumes, current));
    }

    /**
     * Check for expired factor and fallback if it has expired.
     */
    public void checkThrottleFactorValidity() {
        log.info("checking for expired factor");
        try {
            updateFactor(this::maybeFallback);
        } catch (Exception e) {
            log.warn("failed to check for stale factor", e);
        }
    }

    private void updateFactor(Function<ThrottleFactor, ThrottleFactor> throttleFactorUpdater) {
        boolean changed = updateFactorAndCheckIfChanged(throttleFactorUpdater);
        if (changed) {
            listener.run();
        }
    }

    private synchronized boolean updateFactorAndCheckIfChanged(Function<ThrottleFactor, ThrottleFactor> throttleFactorUpdater) {
        ThrottleFactor currentFactor = this.throttleFactor;
        throttleFactor = throttleFactorUpdater.apply(currentFactor);
        boolean changed = !Objects.equals(currentFactor.getThrottleFactor(), throttleFactor.getThrottleFactor());
        if (changed) {
            log.info("Throttle Factor changed from {} to {}, notifying listener", currentFactor, throttleFactor);
        } else {
            log.debug("Throttle Factor unchanged at {}, not notifying listener", throttleFactor);
        }
        return changed;
    }

    private ThrottleFactor getNewFactor(VolumeUsageObservation observedVolumes, ThrottleFactor current) {
        if (observedVolumes.getStatus() == SUCCESS) {
            return calculateThrottleFactorWithExpiry(observedVolumes);
        } else {
            return maybeFallback(current);
        }
    }

    private ThrottleFactor calculateThrottleFactorWithExpiry(VolumeUsageObservation observedVolumes) {
        double newFactor = factorPolicy.calculateFactor(observedVolumes.getVolumeUsages());
        return ThrottleFactor.validFactor(newFactor, clock.instant(), expiryPolicy);
    }

    private ThrottleFactor maybeFallback(ThrottleFactor currentFactor) {
        if (currentFactor.isExpired()) {
            return ThrottleFactor.fallbackThrottleFactor(fallbackThrottleFactor);
        } else {
            return currentFactor;
        }
    }

}


