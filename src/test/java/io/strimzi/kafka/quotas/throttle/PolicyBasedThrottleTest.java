/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedMap;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import io.strimzi.kafka.quotas.StaticQuotaCallback;
import io.strimzi.kafka.quotas.TickableClock;
import io.strimzi.kafka.quotas.VolumeUsage;
import io.strimzi.kafka.quotas.VolumeUsageResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static io.strimzi.kafka.quotas.MetricUtils.METRICS_SCOPE;
import static io.strimzi.kafka.quotas.MetricUtils.assertCounterMetric;
import static io.strimzi.kafka.quotas.MetricUtils.assertGaugeMetric;
import static io.strimzi.kafka.quotas.MetricUtils.getMetricGroup;
import static io.strimzi.kafka.quotas.MetricUtils.resetMetrics;
import static io.strimzi.kafka.quotas.VolumeUsageResult.VolumeSourceObservationStatus.INTERRUPTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

class PolicyBasedThrottleTest {

    public static final ThrottleFactorPolicy THROTTLE_FACTOR_POLICY = observedVolumes -> 1.0;
    private static final String METRICS_TYPE = "ThrottleFactor";

    private final Runnable runnable = Mockito.mock(Runnable.class);

    private final ExpiryPolicy expiryPolicy = Mockito.mock(ExpiryPolicy.class);

    private PolicyBasedThrottle policyBasedThrottle;

    private TickableClock clock;
    private LinkedHashMap<String, String> defaultTags;

    @BeforeEach
    void setUp() {
        resetMetrics(METRICS_SCOPE, METRICS_TYPE);
        clock = new TickableClock();
        defaultTags = new LinkedHashMap<>();
        defaultTags.put(StaticQuotaCallback.HOST_BROKER_TAG, "1");
        policyBasedThrottle = new PolicyBasedThrottle(THROTTLE_FACTOR_POLICY, runnable, clock, expiryPolicy, 0.0, defaultTags);
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

    @Test
    void shouldInitialiseQuotaFactorGauge() {
        //Given

        //When

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(metricsGroup, "ThrottleFactor", defaultTags, 1.0);
    }

    @Test
    void shouldUpdateGaugeWhenFactorChanges() {
        //Given
        givenFactorIsCurrent();

        //When
        whenVolumeUsageResultSuccessObserved();

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertGaugeMetric(metricsGroup, "ThrottleFactor", defaultTags, 1.0);
    }

    @Test
    public void shouldNotIncrementFallbackFactorApplied() {
        //Given
        givenFactorIsCurrent();

        //When
        whenVolumeUsageResultFailureObserved();

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        //Assert zero as the previous factor is still considered valid.
        assertCounterMetric(metricsGroup, "FallbackThrottleFactorApplied", 0L);
    }

    @Test
    public void shouldCountFallbackFactorApplied() {
        //Given
        givenFactorIsExpired();

        //When
        whenCheckForStaleFactor(policyBasedThrottle);

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertCounterMetric(metricsGroup, "FallbackThrottleFactorApplied", 1L);
    }

    @Test
    public void shouldNotCountFallbackFactorAppliedWhenFactorIsValid() {
        //Given
        final FixedDurationExpiryPolicy oneMinuteExpiryPolicy = new FixedDurationExpiryPolicy(clock, Duration.ofMinutes(1L));
        policyBasedThrottle = new PolicyBasedThrottle(THROTTLE_FACTOR_POLICY, runnable, clock, oneMinuteExpiryPolicy, 0.0, defaultTags);

        givenFixedTimeProgressedOneMinute();

        //When
        whenVolumeUsageResultFailureObserved();

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertCounterMetric(metricsGroup, "FallbackThrottleFactorApplied", 0L);
    }

    @Test
    public void shouldIncrementFallbackFactorAppliedWhenPreviousFactorExpires() {
        //Given
        final FixedDurationExpiryPolicy thirtySecondExpiryPolicy = new FixedDurationExpiryPolicy(clock, Duration.ofSeconds(30L));
        policyBasedThrottle = new PolicyBasedThrottle(THROTTLE_FACTOR_POLICY, runnable, clock, thirtySecondExpiryPolicy, 0.0, defaultTags);

        givenFixedTimeProgressedOneMinute();

        //When
        whenVolumeUsageResultFailureObserved();

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertCounterMetric(metricsGroup, "FallbackThrottleFactorApplied", 1L);
    }

    @Test
    public void shouldIncrementFallbackFactorAppliedOnEachExpiry() {
        //Given
        final FixedDurationExpiryPolicy thirtySecondExpiryPolicy = new FixedDurationExpiryPolicy(clock, Duration.ofSeconds(30L));
        policyBasedThrottle = new PolicyBasedThrottle(THROTTLE_FACTOR_POLICY, runnable, clock, thirtySecondExpiryPolicy, 0.0, defaultTags);

        givenFixedTimeProgressedOneMinute();
        whenVolumeUsageResultFailureObserved(); //expiry one

        givenFixedTimeProgressedOneMinute();
        whenVolumeUsageResultSuccessObserved(); //expiry return to normal

        //When
        givenFixedTimeProgressedOneMinute();
        whenVolumeUsageResultFailureObserved(); //expiry two

        //Then
        final SortedMap<MetricName, Metric> metricsGroup = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        assertCounterMetric(metricsGroup, "FallbackThrottleFactorApplied", 2L);
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