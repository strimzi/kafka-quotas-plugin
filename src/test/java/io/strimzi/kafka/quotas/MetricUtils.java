/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricUtils {
    public static final String METRICS_SCOPE = "io.strimzi.kafka.quotas.StaticQuotaCallback";

    /**
     * Static utilities DO NOT instantiate.
     */
    private MetricUtils() {
    }

    public static SortedMap<MetricName, Metric> getMetricGroup(String scope, String type) {
        SortedMap<String, SortedMap<MetricName, Metric>> storageMetrics = getGroupedMetrics(scope, type);
        assertEquals(1, storageMetrics.size(), "unexpected number of metrics in group");
        return storageMetrics.entrySet().iterator().next().getValue();
    }

    public static <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, T expected) {
        assertGaugeMetric(metrics, name, null, expected);
    }

    public static void assertCounterMetric(SortedMap<MetricName, Metric> metrics, String name, long expected) {
        assertCounterMetric(metrics, name, null, expected);
    }

    @SuppressWarnings("unchecked")
    public static <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, LinkedHashMap<String, String> tags, T expected) {
        final String expectedTags = tags != null ? tags.entrySet().stream().map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.joining(",")) : null;
        Optional<Metric> desired = findGaugeMetric(metrics, name, expectedTags);
        assertTrue(desired.isPresent(), String.format("metric with name %s with tags: %s not found in %s", name, expectedTags, metrics));
        Gauge<T> gauge = (Gauge<T>) desired.get();
        assertEquals(expected, gauge.value(), String.format("metric %s has unexpected value", name));
    }

    public static void assertCounterMetric(SortedMap<MetricName, Metric> metrics, String name, LinkedHashMap<String, String> tags, long expected) {
        final String expectedTags = tags != null ? tags.entrySet().stream().map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.joining(",")) : null;
        Optional<Metric> desired = findCounterMetric(metrics, name, expectedTags);
        assertTrue(desired.isPresent(), String.format("metric with name %s with tags: %s not found in %s", name, expectedTags, metrics));
        Counter counter = (Counter) desired.get();
        assertEquals(expected, counter.count(), String.format("metric %s has unexpected value", name));
    }

    public static Optional<Metric> findGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, String expectedTags) {
        return findMetric(metrics, name, expectedTags, Gauge.class);
    }

    public static Optional<Metric> findCounterMetric(SortedMap<MetricName, Metric> metrics, String name, String expectedTags) {
        return findMetric(metrics, name, expectedTags, Counter.class);
    }

    public static <T extends Metric> Optional<Metric> findMetric(SortedMap<MetricName, Metric> metrics, String name, String expectedTags, Class<T> metricType) {
        return metrics.entrySet().stream().filter(e -> {
            final MetricName metricName = e.getKey();
            if (name.equals(metricName.getName())) {
                return expectedTags == null || expectedTags.equals(extractTags(name, metricName));
            } else {
                return false;
            }
        })
        .filter(entry -> metricType.isInstance(entry.getValue()))
        .map(Map.Entry::getValue)
        .findFirst();
    }

    public static void resetMetrics(String scope, String type) {
        final SortedMap<String, SortedMap<MetricName, Metric>> groupedMetrics = getGroupedMetrics(scope, type);
        groupedMetrics.values()
                .forEach(innerMap ->
                        innerMap.forEach((metricName, metric) -> Metrics.defaultRegistry().removeMetric(metricName)));
    }

    private static String extractTags(String name, MetricName metricName) {
        final String mBeanName = metricName.getMBeanName();
        final int nameIndex = mBeanName.lastIndexOf(name);
        return mBeanName.substring(nameIndex + name.length() + 1);
    }

    private static SortedMap<String, SortedMap<MetricName, Metric>> getGroupedMetrics(String scope, String type) {
        return Metrics.defaultRegistry().groupedMetrics((name, metric) -> scope.equals(name.getScope()) && type.equals(name.getType()));
    }
}
