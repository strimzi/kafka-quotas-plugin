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
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

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
     * @param expiryPolicy expiry policy to control how long a factor is applied for
     */
    public PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener, ExpiryPolicy expiryPolicy) {
        this(factorPolicy, listener, Clock.systemUTC(), expiryPolicy);
    }

    PolicyBasedThrottle(ThrottleFactorPolicy factorPolicy, Runnable listener, Clock clock, ExpiryPolicy expiryPolicy) {
        this.factorPolicy = factorPolicy;
        this.listener = listener;
        this.clock = clock;
        this.expiryPolicy = expiryPolicy;
        throttleFactor = ThrottleFactor.validFactor(1.0d, clock.instant(), this.expiryPolicy);
    }

    @Override
    public ThrottleFactor currentThrottleFactor() {
        return throttleFactor;
    }

    @Override
    public void observeVolumeUsage(VolumeUsageObservation observedVolumes) {
        updateFactor(() -> getNewFactor(observedVolumes));
    }

    public void checkForStaleFactor() {
        log.info("checking for stale factor");
        try {
            updateFactor(this::maybeFallback);
        } catch (Exception e) {
            log.warn("failed to check for stale factor", e);
        }
    }

    private void updateFactor(Supplier<ThrottleFactor> throttleFactorSupplier) {
        ThrottleFactor old = this.throttleFactor;
        this.throttleFactor = throttleFactorSupplier.get();
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
            // TODO make fallback factor configurable
            return ThrottleFactor.fallbackThrottleFactor(0.0, Instant.MAX);
        } else {
            return throttleFactor;
        }
    }

}


