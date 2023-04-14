/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import io.strimzi.kafka.quotas.TickableClock;
import io.strimzi.kafka.quotas.VolumeUsage;
import io.strimzi.kafka.quotas.VolumeUsageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static io.strimzi.kafka.quotas.VolumeUsageResult.VolumeSourceObservationStatus.INTERRUPTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

class PolicyBasedThrottleTest {

    public static final ThrottleFactorPolicy THROTTLE_FACTOR_POLICY = observedVolumes -> 1.0;

    private final Runnable runnable = Mockito.mock(Runnable.class);

    private ExpiryPolicy expiryPolicy = Mockito.mock(ExpiryPolicy.class);
    private PolicyBasedThrottle policyBasedThrottle;
    private TickableClock clock;

    @BeforeEach
    void setUp() {
        clock = new TickableClock();
        policyBasedThrottle = new PolicyBasedThrottle(THROTTLE_FACTOR_POLICY, runnable, clock, expiryPolicy, 0.0);
        assertThat(policyBasedThrottle.currentThrottleFactor().getThrottleFactor()).isEqualTo(1.0d);
    }

    @Test
    public void testFallbackWhenFactorExpired() {
        givenFactorIsExpired();
        whenCheckForStaleFactor(policyBasedThrottle);
        thenFactorUpdatedToFallback(policyBasedThrottle);
    }

    @Test
    public void testFallbackWhenFactorExpiredAndFailedVolumeUsageResult() {
        givenFactorIsExpired();
        whenVolumeUsageResultFailureObserved();
        thenFactorUpdatedToFallback(policyBasedThrottle);
    }

    @Test
    public void testCurrentFactorContinuesOnFailedVolumeUsageResult() {
        givenFactorIsCurrent();
        whenVolumeUsageResultFailureObserved();
        thenFactorUnchanged();
    }

    @Test
    public void testCurrentFactorContinuesOnStalenessCheck() {
        givenFactorIsCurrent();
        whenCheckForStaleFactor(policyBasedThrottle);
        thenFactorUnchanged();
    }

    @Test
    public void testCurrentThrottleFactorValidFromUpdatedOnSuccessfulVolumeUsageResult() {
        Instant now = givenFixedTimeProgressedOneMinute();
        whenVolumeUsageResultSuccessObserved();
        thenCurrentThrottleFactorValidFrom(now);
    }

    private void thenCurrentThrottleFactorValidFrom(Instant now) {
        ThrottleFactor throttleFactor = policyBasedThrottle.currentThrottleFactor();
        assertThat(throttleFactor.getValidFrom()).isEqualTo(now);
    }

    private Instant givenFixedTimeProgressedOneMinute() {
        clock.tick(Duration.ofMinutes(1));
        return clock.instant();
    }

    private void whenVolumeUsageResultFailureObserved() {
        policyBasedThrottle.observeVolumeUsage(VolumeUsageResult.failure(INTERRUPTED, null));
    }

    private void whenVolumeUsageResultSuccessObserved() {
        VolumeUsage arbitraryUsage = new VolumeUsage("1", "/tmp", 1000, 1000);
        policyBasedThrottle.observeVolumeUsage(VolumeUsageResult.success(List.of(arbitraryUsage)));
    }

    private void thenFactorUnchanged() {
        assertThat(policyBasedThrottle.currentThrottleFactor().getThrottleFactor()).isEqualTo(1.0d);
        Mockito.verify(runnable, never()).run();
    }

    private void givenFactorIsCurrent() {
        when(expiryPolicy.isExpired(any())).thenReturn(false);
    }


    private void givenFactorIsExpired() {
        when(expiryPolicy.isExpired(any())).thenReturn(true);
    }

    private static void whenCheckForStaleFactor(PolicyBasedThrottle policyBasedThrottle) {
        policyBasedThrottle.checkThrottleFactorValidity();
    }

    private void thenFactorUpdatedToFallback(PolicyBasedThrottle policyBasedThrottle) {
        assertThat(policyBasedThrottle.currentThrottleFactor().getThrottleFactor()).isEqualTo(0.0d);
        Mockito.verify(runnable).run();
    }

}