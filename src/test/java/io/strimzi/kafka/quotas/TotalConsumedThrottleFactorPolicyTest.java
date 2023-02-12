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

@SuppressWarnings("deprecation")
class TotalConsumedThrottleFactorPolicyTest {

    private static final String METRIC_SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";
    private static final Offset<Double> OFFSET = Offset.offset(0.00001d);
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
    public void testHardLimitViolation() {
        Double throttleFactor = throttleFactorPolicy.calculateFactor(List.of(new VolumeUsage("1", "/dir", 1000L, 100L)));

        Assertions.assertThat(throttleFactor).isCloseTo(0.0, OFFSET);
    }

    @Test
    public void testSoftLimitViolation() {
        final TotalConsumedThrottleFactorPolicy factorPolicy = new TotalConsumedThrottleFactorPolicy(900L, 800L);
        Double throttleFactor = factorPolicy.calculateFactor(List.of(new VolumeUsage("1", "/dir", 1000L, 150L)));
        Assertions.assertThat(throttleFactor).isCloseTo(0.5, OFFSET);
    }

    @Test
    public void testSoftLimitViolationLowerBound() {
        TotalConsumedThrottleFactorPolicy factorPolicy = new TotalConsumedThrottleFactorPolicy(900L, 800L);
        Double throttleFactor = factorPolicy.calculateFactor(List.of(new VolumeUsage("1", "/dir", 1000L, 199L)));
        Assertions.assertThat(throttleFactor).isCloseTo(0.99, OFFSET);
    }

    @Test
    public void testSoftLimitViolationUpperBound() {
        TotalConsumedThrottleFactorPolicy factorPolicy = new TotalConsumedThrottleFactorPolicy(900L, 800L);
        Double throttleFactor = factorPolicy.calculateFactor(List.of(new VolumeUsage("1", "/dir", 1000L, 101L)));
        Assertions.assertThat(throttleFactor).isCloseTo(0.01, OFFSET);
    }

    @Test
    public void testHardLimitViolationAcrossMultipleVolumes() {
        Double throttleFactor = throttleFactorPolicy.calculateFactor(List.of(new VolumeUsage("1", "/dir", 1000L, 50L),
                new VolumeUsage("1", "/dir2", 1000L, 50L)));
        Assertions.assertThat(throttleFactor).isCloseTo(0.0, OFFSET);
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
