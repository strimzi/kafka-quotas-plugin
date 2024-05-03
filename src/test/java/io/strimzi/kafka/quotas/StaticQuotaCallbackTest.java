/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.MetricUtils.METRICS_SCOPE;
import static io.strimzi.kafka.quotas.MetricUtils.getMetricGroup;
import static io.strimzi.kafka.quotas.MetricUtils.resetMetrics;
import static io.strimzi.kafka.quotas.VolumeUsageResult.success;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaticQuotaCallbackTest {

    private static final int STORAGE_CHECK_INTERVAL = 20;
    private static final String BROKER_ID_PROPERTY = "broker.id";
    private static final String BROKER_ID = "1";
    private static final Map<String, Object> MINIMUM_EXECUTABLE_CONFIG = Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, String.valueOf(STORAGE_CHECK_INTERVAL), StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092", StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2", BROKER_ID_PROPERTY, BROKER_ID);
    private static final long VOLUME_CAPACITY = 50;
    public static final long THROTTLE_FACTOR_EXPIRY_INTERVAL = 10L;
    private static final String METRICS_TYPE = "StaticQuotaCallback";

    @Mock(lenient = true)
    VolumeSourceBuilder volumeSourceBuilder;

    private static VolumeUsage newVolume(long availableBytes, Instant observedAt) {
        return new VolumeUsage("-1", "test", VOLUME_CAPACITY, availableBytes, observedAt);
    }

    StaticQuotaCallback target;

    ScheduledExecutorService backgroundScheduler = Executors.newSingleThreadScheduledExecutor();

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback();
        when(volumeSourceBuilder.withConfig(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.withVolumeObserver(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.withDefaultTags(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.build()).thenReturn(Mockito.mock(VolumeSource.class));
    }

    @AfterEach
    void tearDown() {
        target.close();
        resetMetrics(METRICS_SCOPE, METRICS_TYPE);
    }

    @Test
    void quotaDefaults() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                BROKER_ID_PROPERTY, BROKER_ID
        ));

        double produceQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, produceQuotaLimit);

        double fetchQuotaLimit = target.quotaLimit(ClientQuotaType.FETCH, target.quotaMetricTags(ClientQuotaType.FETCH, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fetchQuotaLimit);
    }

    @Test
    void produceQuota() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                BROKER_ID_PROPERTY, BROKER_ID_PROPERTY
        ));

        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(1024, quotaLimit);
    }

    @Test
    void shouldNotThrottleToZeroBytes() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, Clock.systemUTC());

        quotaCallback.configure(Map.of(
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "15",
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                BROKER_ID_PROPERTY, BROKER_ID_PROPERTY
        ));

        //When
        argument.getValue().observeVolumeUsage(success(List.of(newVolume(10L, Instant.now()))));

        //Then
        double quotaLimit = quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of());
        assertThat(quotaLimit).isCloseTo(1.0, offset(0.00001d));
    }

    @Test
    void shouldThrottleOnAvailableRatio() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, Clock.systemUTC());

        quotaCallback.configure(Map.of(
                StaticQuotaConfig.AVAILABLE_RATIO_PROP, "0.5",
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                BROKER_ID_PROPERTY, BROKER_ID_PROPERTY
        ));

        //When
        argument.getValue().observeVolumeUsage(success(List.of(new VolumeUsage("-1", "test", 30L, 15L, Instant.now()))));

        //Then
        double quotaLimit = quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of());
        assertThat(quotaLimit).isCloseTo(1.0, offset(0.00001d));
    }

    @Test
    void configuringCheckIntervalWithNoVolumeLimitsDisablesStorageCheck() {
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, executor, Clock.systemUTC());

        quotaCallback.configure(Map.of(
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                BROKER_ID_PROPERTY, BROKER_ID_PROPERTY
        ));

        verifyNoInteractions(volumeSourceBuilder);
        verifyNoInteractions(executor);
    }

    @Test
    void configuringBothPerVolumeLimitTypesNotAllowed() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, Clock.systemUTC());

        //Then
        assertThrows(IllegalStateException.class, () -> quotaCallback.configure(Map.of(
                StaticQuotaConfig.AVAILABLE_RATIO_PROP, "0.5",
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, "1",
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "10",
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092",
                BROKER_ID_PROPERTY, BROKER_ID_PROPERTY
        )));
    }

    @Test
    void excludedPrincipal() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");

        target.configure(Map.of(
                StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "User:foo;User:bar;User:CN=my-cluster,O=io.strimzi;arnost",
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"
        ));

        double fooQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fooQuotaLimit);

        KafkaPrincipal baz = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "baz");
        double bazQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, baz, "clientId"));
        assertEquals(1024, bazQuotaLimit);

        KafkaPrincipal disName = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "CN=my-cluster,O=io.strimzi");
        double disQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, disName, "clientId"));
        assertEquals(Double.MAX_VALUE, disQuotaLimit);

        KafkaPrincipal arnost = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "arnost");
        double arnostQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, arnost, "clientId"));
        assertEquals(1024, arnostQuotaLimit);
    }

    @Test
    void shouldScheduleStorageChecker() {
        //Given
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService, Clock.systemUTC());

        //When
        target.configure(MINIMUM_EXECUTABLE_CONFIG);

        //Verify
        verify(scheduledExecutorService).scheduleWithFixedDelay(any(), eq(0L), eq((long) STORAGE_CHECK_INTERVAL), eq(TimeUnit.SECONDS));
        verify(scheduledExecutorService).scheduleWithFixedDelay(any(), eq(0L), eq(THROTTLE_FACTOR_EXPIRY_INTERVAL), eq(TimeUnit.SECONDS));
    }

    @Test
    void shouldNotScheduleStorageCheckWhenCheckIntervalIsZero() {
        //Given
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService, Clock.systemUTC());

        //When
        target.configure(Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "0", StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        //Then
        verify(scheduledExecutorService, times(0)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldShutdownExecutorOnClose() {
        //Given
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService, Clock.systemUTC());
        target.configure(MINIMUM_EXECUTABLE_CONFIG);

        //When
        target.close();

        //Verify
        verify(scheduledExecutorService, times(1)).shutdownNow();
    }

    @Test
    void observationsForNodesThatDropOutOfActiveSetAreValidUntilExpiry() {
        // given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        TickableClock clock = new TickableClock();
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, clock);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        VolumeObserver volumeObserver = argument.getValue();
        quotaCallback.updateClusterMetadata(null);
        volumeObserver.observeVolumeUsage(success(List.of(new VolumeUsage("0", "dir1", VOLUME_CAPACITY, 0, clock.instant()))));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required after first observation");
        Duration beforeEndOfValidityDuration = Duration.ofMinutes(5).minusNanos(1);
        clock.tick(beforeEndOfValidityDuration);

        // when
        volumeObserver.observeVolumeUsage(success(List.of(new VolumeUsage("1", "dir1", VOLUME_CAPACITY, VOLUME_CAPACITY, clock.instant()))));

        // then
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected reset required after second observation");
        assertEquals(1d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
        quotaCallback.close();
    }

    @Test
    void cachedObservationsExpireAfterValidityDuration() {
        // given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        TickableClock clock = new TickableClock();
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, clock);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        VolumeObserver volumeObserver = argument.getValue();
        quotaCallback.updateClusterMetadata(null);
        volumeObserver.observeVolumeUsage(success(List.of(new VolumeUsage("0", "dir1", VOLUME_CAPACITY, 0, clock.instant()))));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required after first observation");
        clock.tick(Duration.ofMinutes(5));

        // when
        volumeObserver.observeVolumeUsage(success(List.of(new VolumeUsage("1", "dir1", VOLUME_CAPACITY, VOLUME_CAPACITY, clock.instant()))));

        // then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required after second observation");
        assertEquals(Double.MAX_VALUE, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));

        quotaCallback.close();
    }

    @Test
    void quotaResetRequiredShouldRespectQuotaType() {
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, Clock.systemUTC());
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        VolumeObserver volumeObserver = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected initial state");

        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected state on subsequent call without storage state change");

        //When
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(2, Instant.now()))));

        //Then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected state on subsequent call after 1st storage state change");

        quotaCallback.close();
    }

    @Test
    void quotaResetRequired() {
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, Clock.systemUTC());
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        VolumeObserver volumeObserver = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(1, Instant.now()))));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(1, Instant.now()))));
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(3, Instant.now()))));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 2nd storage state change");

        quotaCallback.close();
    }

    @Test
    void staticQuotaMetrics() {

        target.configure(Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 15.0,
                StaticQuotaConfig.FETCH_QUOTA_PROP, 16.0,
                StaticQuotaConfig.REQUEST_QUOTA_PROP, 17.0,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"
        ));

        SortedMap<MetricName, Metric> group = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);

        MetricUtils.assertGaugeMetric(group, "Produce", 15.0);
        MetricUtils.assertGaugeMetric(group, "Fetch", 16.0);
        MetricUtils.assertGaugeMetric(group, "Request", 17.0);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldProduceValidMbeanObjectNamesWhenGroupContains(String name, String illegalPattern) {
        //Given

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource", "group" + illegalPattern);

        //Then
        assertThat(metricName.getGroup()).isEqualTo("group");
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldProduceValidMbeanObjectNamesWhenTypeContains(String name, String illegalPattern) {
        //Given

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource" + illegalPattern, "group");

        //Then
        assertThat(metricName.getType()).isEqualTo("VolumeSource");
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldProduceValidMbeanObjectNamesWhenTypeClassContains(String name, String illegalPattern) {
        //Given

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource" + illegalPattern, "group");

        //Then
        assertThat(metricName.getType()).isEqualTo("VolumeSource");
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldProduceValidMbeanObjectNamesWhenGroupContainsWithTags(String name, String illegalPattern) {
        //Given

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource", "group" + illegalPattern, new LinkedHashMap<>());

        //Then
        assertThat(metricName.getGroup()).isEqualTo("group");
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldProduceValidMbeanObjectNamesWhenTypeContainsWithTags(String name, String illegalPattern) {
        //Given

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource" + illegalPattern, "group", new LinkedHashMap<>());

        //Then
        assertThat(metricName.getType()).isEqualTo("VolumeSource");
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldProduceValidMbeanObjectNamesWhenTypeClassContainsWithTags(String name, String illegalPattern) {
        //Given

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource" + illegalPattern, "group", new LinkedHashMap<>());

        //Then
        assertThat(metricName.getType()).isEqualTo("VolumeSource");
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(value = {"colon,:", "double forward slashes,//", "asterisk,*", "question mark,?",  "comma,','", "equals,="})
    void shouldSanitiseTagValues(String name, String illegalPattern) {
        //Given
        final LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("key1", "value" + illegalPattern);
        tags.put("key2", "value2");

        //When
        final MetricName metricName = StaticQuotaCallback.metricName("class", "VolumeSource" + illegalPattern, "group", tags);

        //Then
        assertMetricNameIsValid(illegalPattern, metricName.getMBeanName(), tags.size());
    }

    private static void assertMetricNameIsValid(String illegalPattern, String mBeanName) {
        assertMetricNameIsValid(illegalPattern, mBeanName, 0);
    }
    private static void assertMetricNameIsValid(String illegalPattern, String mBeanName, int tagCount) {
        String domain = mBeanName.substring(0, mBeanName.indexOf(":"));
        String keyProperties = mBeanName.substring(mBeanName.indexOf(":") + 1);
        assertThat(domain).doesNotContain(illegalPattern);
        if (illegalPattern.equals(",")) {
            assertThat(keyProperties.split(illegalPattern)).hasSize(2 + tagCount);
        } else if (illegalPattern.equals("=")) {
            assertThat(keyProperties.split("=")).hasSize(3 + tagCount);
        } else {
            assertThat(keyProperties).doesNotContain(illegalPattern);
        }
    }
}
