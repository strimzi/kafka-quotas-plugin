/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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

        // check that specifying principal without User: prefix will throw an exception
        assertThrows(ConfigException.class, () -> target.configure(Map.of(
            StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "User:foo;User:bar;User:CN=my-cluster,O=io.strimzi;arnost",
            StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
            StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"
        )));

        target.configure(Map.of(
                StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "User:foo;User:bar;User:CN=my-cluster,O=io.strimzi",
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
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect the first change to be reported once more");

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
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, new TickableClock());
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

    @Test
    void reconfigurableConfigsAreTheQuotaLimits() {
        assertThat(target.reconfigurableConfigs()).containsExactlyInAnyOrder(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP,
                StaticQuotaConfig.FETCH_QUOTA_PROP,
                StaticQuotaConfig.REQUEST_QUOTA_PROP);
    }

    @Test
    void reconfigureChangesQuotaLimitForExistingClient() {
        //Given a client whose quota limit Kafka has already read, with no reset pending
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, new TickableClock());
        quotaCallback.configure(quotaLimits("1024"));
        Map<String, String> metricTags = quotaCallback.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId");
        assertEquals(1024d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, metricTags));
        for (ClientQuotaType quotaType : ClientQuotaType.values()) {
            quotaCallback.quotaResetRequired(quotaType);
        }
        Map<String, Object> config = new HashMap<>(quotaLimits("1024"));
        config.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "2048");

        //When
        quotaCallback.reconfigure(config);

        //Then Kafka is asked to read the produce limit again for every existing client
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required for PRODUCE");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected reset required again before the delay");
        for (ClientQuotaType unchanged : List.of(ClientQuotaType.FETCH, ClientQuotaType.REQUEST, ClientQuotaType.CONTROLLER_MUTATION)) {
            assertFalse(quotaCallback.quotaResetRequired(unchanged), "unexpected reset required for " + unchanged);
        }
        assertEquals(2048d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, metricTags));
        quotaCallback.close();
    }

    @Test
    void quotaChangeIsReportedOnceMoreAfterADelay() {
        //Given two changes, each picked up by a different request thread
        TickableClock clock = new TickableClock();
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, clock);
        quotaCallback.configure(quotaLimits("1024"));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        quotaCallback.reconfigure(quotaLimits("2048"));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required after the first change");
        double readByFirstThread = quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of());
        quotaCallback.reconfigure(quotaLimits("4096"));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required after the second change");
        // the first thread now installs the value it read, after the second thread installed the current one
        assertEquals(2048d, readByFirstThread);
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected reset required before the delay");

        //When
        clock.tick(StaticQuotaCallback.RESET_REPEAT_DELAY);

        //Then Kafka is asked exactly once more, and reads the current limit
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect the reset to be reported once more");
        assertEquals(4096d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
        clock.tick(StaticQuotaCallback.RESET_REPEAT_DELAY);
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected further reset required");
        quotaCallback.close();
    }

    @Test
    void throttleFactorChangeIsReportedOnceMoreAfterADelay() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        TickableClock clock = new TickableClock();
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, mock(ScheduledExecutorService.class), clock);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        argument.getValue().observeVolumeUsage(success(List.of(newVolume(1, clock.instant()))));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect reset required after the throttle factor changed");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected reset required before the delay");

        //When
        clock.tick(StaticQuotaCallback.RESET_REPEAT_DELAY);

        //Then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "expect the reset to be reported once more");
        clock.tick(StaticQuotaCallback.RESET_REPEAT_DELAY);
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected further reset required");
        quotaCallback.close();
    }

    @Test
    void removingDynamicLimitRestoresStaticLimit() {
        //Given
        Map<String, Object> staticConfig = Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "1024", StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092");
        target.configure(staticConfig);
        Map<String, Object> dynamicConfig = new HashMap<>(staticConfig);
        dynamicConfig.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "2048");
        target.reconfigure(dynamicConfig);
        assertEquals(2048d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));

        //When the dynamic value is deleted, Kafka passes the static value again
        target.reconfigure(staticConfig);

        //Then
        assertEquals(1024d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
    }

    @Test
    void removingDynamicLimitWithoutStaticLimitRestoresDefault() {
        //Given
        Map<String, Object> staticConfig = Map.of(StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092");
        target.configure(staticConfig);
        target.reconfigure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "2048", StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));
        assertEquals(2048d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));

        //When the dynamic value is deleted, Kafka passes no value for the key
        target.reconfigure(staticConfig);

        //Then
        assertEquals(Double.MAX_VALUE, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
    }

    @Test
    void invalidQuotaLimitIsRejectedAndNoLimitChanges() {
        //Given
        target.configure(quotaLimits("1024"));
        Map<String, Object> invalidConfig = new HashMap<>(quotaLimits("2048"));
        invalidConfig.put(StaticQuotaConfig.FETCH_QUOTA_PROP, "fast");

        //When
        assertThrows(ConfigException.class, () -> target.validateReconfiguration(invalidConfig));
        assertThrows(ConfigException.class, () -> target.reconfigure(invalidConfig));

        //Then
        assertEquals(1024d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
        assertEquals(1024d, target.quotaLimit(ClientQuotaType.FETCH, Map.of()));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"0", "-1"})
    void nonPositiveQuotaLimitInThePropertiesFileMeansOneBytePerSecond(String limit) {
        //When
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, limit, StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        //Then the limit is raised to the minimum of 1 byte per second
        assertEquals(1d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"0", "-1"})
    void nonPositiveQuotaLimitIsAcceptedAtRuntimeAsInThePropertiesFile(String limit) {
        //Given
        target.configure(quotaLimits("1024"));
        Map<String, Object> config = quotaLimits(limit);

        //When
        assertDoesNotThrow(() -> target.validateReconfiguration(config));
        target.reconfigure(config);

        //Then the limit is raised to the minimum of 1 byte per second, as for the same value in the properties file
        assertEquals(1d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"NaN", "Infinity", "-Infinity"})
    void nonFiniteQuotaLimitIsAcceptedAtRuntimeAsInThePropertiesFile(String limit) {
        //Given
        target.configure(quotaLimits("1024"));
        Map<String, Object> config = quotaLimits(limit);

        //When
        assertDoesNotThrow(() -> target.validateReconfiguration(config));
        target.reconfigure(config);

        //Then every quota is the minimum of 1 byte per second, as for the same value in the properties file
        for (ClientQuotaType quotaType : List.of(ClientQuotaType.PRODUCE, ClientQuotaType.FETCH, ClientQuotaType.REQUEST)) {
            assertEquals(1d, target.quotaLimit(quotaType, Map.of()), "unexpected limit for " + quotaType);
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"NaN", "Infinity"})
    void nonFiniteQuotaLimitInThePropertiesFileMeansOneBytePerSecond(String limit) {
        //When
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, limit, StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        //Then the limit is the minimum of 1 byte per second, not unlimited
        assertEquals(1d, target.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
    }

    @Test
    void infiniteQuotaLimitWithThrottleFactorZeroMeansOneBytePerSecond() {
        //Given an infinite produce limit in the properties file
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        TickableClock clock = new TickableClock();
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, mock(ScheduledExecutorService.class), clock);
        Map<String, Object> config = new HashMap<>(MINIMUM_EXECUTABLE_CONFIG);
        config.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "Infinity");
        quotaCallback.configure(config);

        //When the disk guard sets the throttle factor to 0
        argument.getValue().observeVolumeUsage(success(List.of(newVolume(1, clock.instant()))));

        //Then producers are limited to 1 byte per second, not NaN (which Kafka would not throttle at all)
        assertEquals(1d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
        quotaCallback.close();
    }

    @ParameterizedTest(name = "cluster default {0}")
    @CsvSource({"0, 1", "5000, 5000"})
    void brokersWithDifferentStaticLimitsApplyAClusterDefaultAndItsRemoval(String clusterDefault, double limitWithClusterDefault) {
        for (String staticLimit : List.of("0", "1024")) {
            //Given a broker with its own static produce limit
            StaticQuotaCallback broker = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler, new TickableClock());
            Map<String, Object> staticConfig = Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, staticLimit, StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092");
            broker.configure(staticConfig);
            double staticLimitInForce = broker.quotaLimit(ClientQuotaType.PRODUCE, Map.of());
            Map<String, Object> withClusterDefault = new HashMap<>(staticConfig);
            withClusterDefault.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, clusterDefault);

            //When the cluster default is set and then deleted
            //Then the broker validates and applies both transitions
            assertDoesNotThrow(() -> validateAndApply(broker, withClusterDefault), "setting the cluster default with static limit " + staticLimit);
            assertEquals(limitWithClusterDefault, broker.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
            assertDoesNotThrow(() -> validateAndApply(broker, staticConfig), "deleting the cluster default with static limit " + staticLimit);
            assertEquals(staticLimitInForce, broker.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
            broker.close();
        }
    }

    @Test
    void staticZeroQuotaLimitDoesNotBlockAnotherRuntimeChange() {
        //Given
        Map<String, Object> staticConfig = Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "0", StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092");
        target.configure(staticConfig);
        Map<String, Object> config = new HashMap<>(staticConfig);
        config.put(StaticQuotaConfig.FETCH_QUOTA_PROP, "2048");

        //Then
        assertDoesNotThrow(() -> target.validateReconfiguration(config));
    }

    @Test
    void reconfigureUpdatesQuotaMetricsWithoutRegisteringNewOnes() {
        //Given
        target.configure(quotaLimits("15.0"));
        Set<MetricName> registeredBefore = new HashSet<>(Metrics.defaultRegistry().allMetrics().keySet());

        //When
        target.reconfigure(quotaLimits("25.0"));

        //Then
        assertEquals(registeredBefore, Metrics.defaultRegistry().allMetrics().keySet());
        SortedMap<MetricName, Metric> group = getMetricGroup(METRICS_SCOPE, METRICS_TYPE);
        MetricUtils.assertGaugeMetric(group, "Produce", 25.0);
        MetricUtils.assertGaugeMetric(group, "Fetch", 25.0);
        MetricUtils.assertGaugeMetric(group, "Request", 25.0);
    }

    @Test
    void reconfigureKeepsThrottleFactorInForce() {
        //Given the fallback factor of 0.5 is applied because the last observation expired
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        TickableClock clock = new TickableClock();
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, executor, clock);
        Map<String, Object> config = new HashMap<>(MINIMUM_EXECUTABLE_CONFIG);
        config.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "1024");
        config.put(StaticQuotaConfig.FALLBACK_THROTTLE_FACTOR_PROP, "0.5");
        quotaCallback.configure(config);
        ArgumentCaptor<Runnable> throttleFactorValidityCheck = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleWithFixedDelay(throttleFactorValidityCheck.capture(), eq(0L), eq(THROTTLE_FACTOR_EXPIRY_INTERVAL), eq(TimeUnit.SECONDS));
        clock.tick(Duration.ofMinutes(6));
        throttleFactorValidityCheck.getValue().run();
        assertEquals(512d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));

        //When
        config.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "2048");
        quotaCallback.reconfigure(config);

        //Then the factor still applies to the new limit (a new throttle would give 2048) and the storage check was not restarted
        assertEquals(1024d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
        MetricUtils.assertGaugeMetric(getMetricGroup(METRICS_SCOPE, METRICS_TYPE), "Produce", 2048.0);
        verify(executor, times(2)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
        verify(volumeSourceBuilder, times(1)).build();

        //When the factor brings the new limit below 1 byte per second
        config.put(StaticQuotaConfig.PRODUCE_QUOTA_PROP, "1");
        quotaCallback.reconfigure(config);

        //Then the limit is raised to the minimum of 1 byte per second
        assertEquals(1d, quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of()));
        quotaCallback.close();
    }

    @Test
    void quotaLimitSeesEitherOldOrNewLimitsDuringReconfigure() throws Exception {
        //Given
        Map<String, Object> lowLimits = quotaLimits("1000");
        Map<String, Object> highLimits = quotaLimits("2000");
        target.configure(lowLimits);
        ExecutorService reader = Executors.newSingleThreadExecutor();
        AtomicBoolean reconfiguring = new AtomicBoolean(true);
        try {
            CountDownLatch readerStarted = new CountDownLatch(1);
            Future<Set<Double>> observedLimits = reader.submit(() -> {
                Set<Double> observed = new HashSet<>();
                readerStarted.countDown();
                while (reconfiguring.get()) {
                    for (ClientQuotaType quotaType : List.of(ClientQuotaType.PRODUCE, ClientQuotaType.FETCH, ClientQuotaType.REQUEST)) {
                        observed.add(target.quotaLimit(quotaType, Map.of()));
                    }
                }
                return observed;
            });

            //When
            readerStarted.await();
            for (int i = 0; i < 10_000; i++) {
                target.reconfigure(i % 2 == 0 ? highLimits : lowLimits);
            }
            reconfiguring.set(false);

            //Then
            assertThat(observedLimits.get(10, TimeUnit.SECONDS)).isSubsetOf(1000d, 2000d);
        } finally {
            reconfiguring.set(false);
            reader.shutdown();
        }
    }

    // What Kafka does on each broker when it applies a dynamic configuration change
    private static void validateAndApply(StaticQuotaCallback callback, Map<String, Object> configs) {
        callback.validateReconfiguration(configs);
        callback.reconfigure(configs);
    }

    private static Map<String, Object> quotaLimits(String limit) {
        return Map.of(
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, limit,
                StaticQuotaConfig.FETCH_QUOTA_PROP, limit,
                StaticQuotaConfig.REQUEST_QUOTA_PROP, limit,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092");
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
