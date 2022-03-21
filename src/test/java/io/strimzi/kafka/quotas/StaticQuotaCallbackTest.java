/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.function.Consumer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class StaticQuotaCallbackTest {

    private static final double EPSILON = 1E-5;
    private static final long ONE_GB = 1_073_741_824L;
    private static final long TWO_GB = 2 * ONE_GB;
    private static final long NINE_GB = 9 * ONE_GB;
    private static final long TEN_GB = 10 * ONE_GB;
    public static final VolumeDetails TEN_PERCENT_USED_VOLUME = new VolumeDetails("Test", TEN_GB, ONE_GB);

    StaticQuotaCallback target;

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback();
    }

    @AfterEach
    void tearDown() {
        target.close();
    }

    @Test
    void quotaDefaults() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of());

        double produceQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, produceQuotaLimit);

        double fetchQuotaLimit = target.quotaLimit(ClientQuotaType.FETCH, target.quotaMetricTags(ClientQuotaType.FETCH, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fetchQuotaLimit);
    }

    @Test
    void produceQuota() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));

        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(1024, quotaLimit);
    }

    @Test
    void excludedPrincipal() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "foo,bar",
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024));
        double fooQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fooQuotaLimit);

        KafkaPrincipal baz = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "baz");
        double bazQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, baz, "clientId"));
        assertEquals(1024, bazQuotaLimit);
    }

    @Test
    void pluginLifecycle() throws Exception {
        StorageChecker mock = mock(StorageChecker.class);
        StaticQuotaCallback target = new StaticQuotaCallback(mock);
        target.configure(Map.of());
        target.updateClusterMetadata(null);
        verify(mock, times(1)).startIfNecessary();
        target.close();
        verify(mock, times(1)).stop();
    }

    @SuppressWarnings("unchecked")
    @Test
    void quotaResetRequired() {
        //Given
        StorageChecker mock = mock(StorageChecker.class);
        ArgumentCaptor<Consumer<Map<String, VolumeDetails>>> argument = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(mock).configure(anyLong(), anyList(), argument.capture());
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(mock);
        quotaCallback.configure(Map.of());
        Consumer<Map<String, VolumeDetails>> storageUpdateConsumer = argument.getValue();
        quotaCallback.updateClusterMetadata(null);
        quotaCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));
        final Map<String, VolumeDetails> underQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(8L));
        final Map<String, VolumeDetails> overQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(12L));

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");

        //When
        storageUpdateConsumer.accept(overQuotaUsage); //Need to use over quota here otherwise the factor doesn't change

        //Then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");

        //When
        storageUpdateConsumer.accept(overQuotaUsage);

        //Then
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");

        //When
        storageUpdateConsumer.accept(underQuotaUsage);

        //Then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 2nd storage state change");

        quotaCallback.close();
    }

    @Test
    void storageCheckerMetrics() {
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 15L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 16L
        ));

        target.calculateQuotaFactor(Map.of("Disk one", newVolumeWithConsumedCapacity(17L)));

        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StorageChecker");

        assertGaugeMetric(group, "SoftLimitBytes", 15L);
        assertGaugeMetric(group, "HardLimitBytes", 16L);
        assertGaugeMetric(group, "TotalStorageUsedBytes", 17L);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StorageChecker,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");
    }

    @Test
    void staticQuotaMetrics() {

        target.configure(Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 15.0,
                StaticQuotaConfig.FETCH_QUOTA_PROP, 16.0,
                StaticQuotaConfig.REQUEST_QUOTA_PROP, 17.0
        ));

        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StaticQuotaCallback");

        assertGaugeMetric(group, "Produce", 15.0);
        assertGaugeMetric(group, "Fetch", 16.0);
        assertGaugeMetric(group, "Request", 17.0);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");
    }

    @Test
    void shouldSetQuotaFactorToZeroIfHardLimitBreached() {
        //Given
        final Map<String, VolumeDetails> diskUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(20L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(diskUsage);

        //Then
        assertEquals(0.0D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldSetQuotaFactorToZeroIfUsageIsEqualToHardLimit() {
        //Given
        final Map<String, VolumeDetails> diskUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(15L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(diskUsage);

        //Then
        assertEquals(0.0D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldReduceQuotaFactorIfUsageBetweenSoftAndHardLimits() {
        //Given
        final Map<String, VolumeDetails> usagePerDisk = Map.of("Disk one", newVolumeWithConsumedCapacity(12L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(usagePerDisk);

        //Then
        assertEquals(0.6D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldSetQuotaFactorToZeroIfHardLimitBreachedForAnyDisk() {
        //Given
        final Map<String, VolumeDetails> diskUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(8L), "Disk Two", newVolumeWithConsumedCapacity(20L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(diskUsage);

        //Then
        assertEquals(0.0D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldReduceQuotaFactorIfUsageBetweenSoftAndHardLimitsForOneDisk() {
        //Given
        final Map<String, VolumeDetails> usagePerDisk = Map.of("Disk one", newVolumeWithConsumedCapacity(8L), "Disk Two", newVolumeWithConsumedCapacity(12L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(usagePerDisk);

        //Then
        assertEquals(0.6D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldUseTheSmallestFactorWhenMultipleDisksAreOverTheSoftLimit() {
        //Given
        final Map<String, VolumeDetails> usagePerDisk = Map.of("Disk one", newVolumeWithConsumedCapacity(14L), "Disk Two", newVolumeWithConsumedCapacity(12L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(usagePerDisk);

        //Then
        assertEquals(0.2D, target.currentQuotaFactor, EPSILON);
        //Disk one gets a factor of 0.2
        //Disk two gets a factor of 0.6
    }

    @Test
    void shouldApplyHardLimitInPreferenceToSoftLimit() {
        //Given
        final Map<String, VolumeDetails> diskUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(12L), "Disk Two", newVolumeWithConsumedCapacity(20L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(diskUsage);

        //Then
        assertEquals(0.0D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldSetQuotaFactorToOneIfUsageBelowSoftAndHardLimits() {
        //Given
        final Map<String, VolumeDetails> diskUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(8L));
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        //When
        target.calculateQuotaFactor(diskUsage);

        //Then
        assertEquals(1.0D, target.currentQuotaFactor, EPSILON);
    }

    @Test
    void shouldMarkQuotaResetWhenQuotaFactorChanges() {
        //Given
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));
        final Map<String, VolumeDetails> underQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(8L));
        final Map<String, VolumeDetails> overQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(12L));
        target.calculateQuotaFactor(underQuotaUsage);
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE)); //quotaResetRequired resets the flag to false
        assertFalse(target.quotaResetRequired(ClientQuotaType.PRODUCE));

        //When
        target.calculateQuotaFactor(overQuotaUsage);

        //Then
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE));
    }

    @Test
    void shouldMarkQuotaResetWhenQuotaFactorChangesFromHardToSoft() {
        //Given
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));
        final Map<String, VolumeDetails> underQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(16L));
        final Map<String, VolumeDetails> overQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(12L));
        target.calculateQuotaFactor(underQuotaUsage);
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE)); //quotaResetRequired resets the flag to false
        assertFalse(target.quotaResetRequired(ClientQuotaType.PRODUCE));

        //When
        target.calculateQuotaFactor(overQuotaUsage);

        //Then
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE));
    }

    @Test
    void shouldNotMarkQuotaResetWhenQuotaFactorUnchanged() {
        //Given
        target.configure(Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));
        final Map<String, VolumeDetails> underQuotaUsage = Map.of("Disk one", newVolumeWithConsumedCapacity(8L));
        target.calculateQuotaFactor(underQuotaUsage);
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE));

        //When
        target.calculateQuotaFactor(underQuotaUsage);

        //Then
        assertFalse(target.quotaResetRequired(ClientQuotaType.PRODUCE));
    }

    @Test
    void shouldReduceProduceQuotaByQuotaFactor() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        final Map<String, VolumeDetails> overSoftQuota = Map.of("Disk one", newVolumeWithConsumedCapacity(12L));
        target.calculateQuotaFactor(overSoftQuota);
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE));

        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        //1024 * 0.6 = 614.4
        assertEquals(614.4, quotaLimit, EPSILON);
    }

    @Test
    void shouldBlockProduceWhenHardLimitBreached() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_PROP, 15L
        ));

        final Map<String, VolumeDetails> overHardQuota = Map.of("Disk one", newVolumeWithConsumedCapacity(16L));
        target.calculateQuotaFactor(overHardQuota);
        assertTrue(target.quotaResetRequired(ClientQuotaType.PRODUCE));

        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(1.0, quotaLimit, EPSILON);
    }

    public static SortedMap<MetricName, Metric> getMetricGroup(String scope, String type) {
        SortedMap<String, SortedMap<MetricName, Metric>> storageMetrics = Metrics.defaultRegistry().groupedMetrics((name, metric) -> scope.equals(name.getScope()) && type.equals(name.getType()));
        assertEquals(1, storageMetrics.size(), "unexpected number of metrics in group");
        return storageMetrics.entrySet().iterator().next().getValue();
    }

    private VolumeDetails newVolumeWithConsumedCapacity(long consumedCapacity) {
        return new VolumeDetails("TestVolume", 100L, consumedCapacity);
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
