/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetricUtils {
    /**
     * Static utilities DO NOT instantiate.
     */
    private MetricUtils() {
    }

    public static SortedMap<MetricName, Metric> getMetricGroup(String scope, String type) {
        SortedMap<String, SortedMap<MetricName, Metric>> storageMetrics = Metrics.defaultRegistry().groupedMetrics((name, metric) -> scope.equals(name.getScope()) && type.equals(name.getType()));
        assertEquals(1, storageMetrics.size(), "unexpected number of metrics in group");
        return storageMetrics.entrySet().iterator().next().getValue();
    }

    public static <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, T expected) {
        assertGaugeMetric(metrics, name, null, expected);
    }

    public static <T> void assertGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, LinkedHashMap<String, String> tags, T expected) {
        final String expectedTags = tags != null ? tags.entrySet().stream().map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue())).collect(Collectors.joining(",")) : null;
        Optional<Gauge<T>> desired = findGaugeMetric(metrics, name, expectedTags);
        assertTrue(desired.isPresent(), String.format("metric with name %s with tags: %s not found in %s", name, expectedTags, metrics));
        Gauge<T> gauge = desired.get();
        assertEquals(expected, gauge.value(), String.format("metric %s has unexpected value", name));
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<Gauge<T>> findGaugeMetric(SortedMap<MetricName, Metric> metrics, String name, String expectedTags) {
        return metrics.entrySet().stream().filter(e -> {
            final MetricName metricName = e.getKey();
            if (name.equals(metricName.getName())) {
                return expectedTags == null || expectedTags.equals(extractTags(name, metricName));
            } else {
                return false;
            }
        }).map(e -> (Gauge<T>) e.getValue()).findFirst();
    }

    private static String extractTags(String name, MetricName metricName) {
        final String mBeanName = metricName.getMBeanName();
        final int nameIndex = mBeanName.lastIndexOf(name);
        return mBeanName.substring(nameIndex + name.length() + 1);
    }
}
