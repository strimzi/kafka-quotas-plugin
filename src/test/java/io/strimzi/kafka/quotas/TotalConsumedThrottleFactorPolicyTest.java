/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.List;
import java.util.Optional;
import java.util.SortedMap;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@SuppressWarnings("deprecation")
class TotalConsumedThrottleFactorPolicyTest {

    private static final String METRIC_SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";
    private TotalConsumedThrottleFactorPolicy throttleFactorPolicy;

    @AfterEach
    public void resetMetrics() {
        Metrics.defaultRegistry().allMetrics().keySet().stream().filter(m -> METRIC_SCOPE.equals(m.getScope())).forEach(Metrics.defaultRegistry()::removeMetric);
    }

    @BeforeEach
    void setUp() {
        throttleFactorPolicy = new TotalConsumedThrottleFactorPolicy(900L, 900L);
    }

    @Test
    public void testListenersNotifiedOnChange() {
        Runnable runnable = mock(Runnable.class);
        throttleFactorPolicy.addUpdateListener(runnable);
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));
        verify(runnable).run();
    }

    @Test
    public void testListenerNotNotifiedIfTotalConsumedUnchanged() {
        Runnable runnable = mock(Runnable.class);
        throttleFactorPolicy.addUpdateListener(runnable);
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));
        verify(runnable).run();
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));
        verifyNoMoreInteractions(runnable);
    }

    @Test
    public void testListenerNotifiedIfTotalConsumedChanged() {
        Runnable runnable = mock(Runnable.class);
        throttleFactorPolicy.addUpdateListener(runnable);
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));
        verify(runnable).run();
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 50L)));
        verify(runnable, times(2)).run();
        verifyNoMoreInteractions(runnable);
    }

    @Test
    public void testHardLimitViolation() {
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));
        Double throttleFactor = throttleFactorPolicy.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.0, Offset.offset(0.00001d));
    }

    @Test
    public void testSoftLimitViolation() {
        TotalConsumedThrottleFactorPolicy supplier = new TotalConsumedThrottleFactorPolicy(900L, 800L);
        supplier.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 150L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.5, Offset.offset(0.00001d));
    }

    @Test
    public void testSoftLimitViolationLowerBound() {
        TotalConsumedThrottleFactorPolicy supplier = new TotalConsumedThrottleFactorPolicy(900L, 800L);
        supplier.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 199L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.99, Offset.offset(0.00001d));
    }

    @Test
    public void testSoftLimitViolationUpperBound() {
        TotalConsumedThrottleFactorPolicy supplier = new TotalConsumedThrottleFactorPolicy(900L, 800L);
        supplier.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 101L)));
        Double throttleFactor = supplier.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.01, Offset.offset(0.00001d));
    }

    @Test
    public void testHardLimitViolationAcrossMultipleVolumes() {
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 50L),
                new VolumeUsage("1", "/dir2", 1000L, 50L)));
        Double throttleFactor = throttleFactorPolicy.get();
        Assertions.assertThat(throttleFactor).isCloseTo(0.0, Offset.offset(0.00001d));
    }

    @Test
    public void testHardLimitViolationRecovery() {
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));
        throttleFactorPolicy.observeVolumeUsage(List.of(new VolumeUsage("1", "/dir", 1000L, 1000L)));
        Double throttleFactor = throttleFactorPolicy.get();
        Assertions.assertThat(throttleFactor).isCloseTo(1.0, Offset.offset(0.00001d));
    }

    @Test
    public void testThrottleFactorDefaultsToOpen() {
        Double throttleFactor = throttleFactorPolicy.get();
        Assertions.assertThat(throttleFactor).isCloseTo(1.0, Offset.offset(0.00001d));
    }

    @Test
    void shouldCreateLimitMetrics() {
        //Given
        resetMetrics(); //Needed as setup creates a policy which registers metrics
        final long consumedBytesSoftLimit = 800L;
        final long consumedBytesHardLimit = 900L;

        //When
        new TotalConsumedThrottleFactorPolicy(consumedBytesHardLimit, consumedBytesSoftLimit);

        //Then
        SortedMap<MetricName, Metric> group = getMetricGroup("StorageChecker");

        assertGaugeMetric(group, "SoftLimitBytes", consumedBytesSoftLimit);
        assertGaugeMetric(group, "HardLimitBytes", consumedBytesHardLimit);
        assertGaugeMetric(group, "TotalStorageUsedBytes", 0L);
    }

    //TODO move to shared place
    private SortedMap<MetricName, Metric> getMetricGroup(String t) {
        SortedMap<String, SortedMap<MetricName, Metric>> storageMetrics = Metrics.defaultRegistry().groupedMetrics((name, metric) -> METRIC_SCOPE.equals(name.getScope()) && t.equals(name.getType()));
        assertEquals(1, storageMetrics.size(), "unexpected number of metrics in group");
        return storageMetrics.entrySet().iterator().next().getValue();
    }

    private <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, T expected) {
        Optional<Gauge<T>> desired = findGaugeMetric(metrics, name);
        assertTrue(desired.isPresent(), String.format("metric with name %s not found in %s", name, metrics));
        Gauge<T> gauge = desired.get();
        assertEquals(expected, gauge.value(), String.format("metric %s has unexpected value", name));
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<Gauge<T>> findGaugeMetric(SortedMap<MetricName, Metric> metrics, String name) {
        return metrics.entrySet().stream().filter(e -> name.equals(e.getKey().getName())).map(e -> (Gauge<T>) e.getValue()).findFirst();
    }
}
