/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.strimzi.kafka.quotas.MetricUtils.getMetricGroup;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StaticQuotaCallbackTest {

    private static final int STORAGE_CHECK_INTERVAL = 20;
    private static final Map<String, Object> MINIMUM_EXECUTABLE_CONFIG = Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, STORAGE_CHECK_INTERVAL, StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092", StaticQuotaConfig.AVAILABLE_BYTES_PROP, "2");
    private static final long VOLUME_CAPACITY = 50;
    public static final long THROTTLE_FACTOR_EXPIRY_INTERVAL = 10L;

    @Mock(lenient = true)
    VolumeSourceBuilder volumeSourceBuilder;

    private static VolumeUsage newVolume(long availableBytes) {
        return new VolumeUsage("-1", "test", VOLUME_CAPACITY, availableBytes);
    }

    StaticQuotaCallback target;

    ScheduledExecutorService backgroundScheduler = Executors.newSingleThreadScheduledExecutor();

    @BeforeEach
    void setup() {
        target = new StaticQuotaCallback();
        when(volumeSourceBuilder.withConfig(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.withVolumeObserver(any())).thenReturn(volumeSourceBuilder);
        when(volumeSourceBuilder.build()).thenReturn(Mockito.mock(VolumeSource.class));
    }

    @AfterEach
    void tearDown() {
        target.close();
    }

    @Test
    void quotaDefaults() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        double produceQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, produceQuotaLimit);

        double fetchQuotaLimit = target.quotaLimit(ClientQuotaType.FETCH, target.quotaMetricTags(ClientQuotaType.FETCH, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fetchQuotaLimit);
    }

    @Test
    void produceQuota() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        double quotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(1024, quotaLimit);
    }

    @Test
    void shouldNotThrottleToZeroBytes() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler);

        quotaCallback.configure(Map.of(
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, 15L,
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, 10,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"
        ));

        //When
        argument.getValue().observeVolumeUsage(success(List.of(newVolume(10L))));

        //Then
        double quotaLimit = quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of());
        assertThat(quotaLimit).isCloseTo(1.0, offset(0.00001d));
    }

    @Test
    void shouldThrottleOnAvailableRatio() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler);

        quotaCallback.configure(Map.of(
                StaticQuotaConfig.AVAILABLE_RATIO_PROP, 0.5,
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, 10,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"
        ));

        //When
        argument.getValue().observeVolumeUsage(success(List.of(new VolumeUsage("-1", "test", 30L, 15L))));

        //Then
        double quotaLimit = quotaCallback.quotaLimit(ClientQuotaType.PRODUCE, Map.of());
        assertThat(quotaLimit).isCloseTo(1.0, offset(0.00001d));
    }

    @Test
    void configuringBothPerVolumeLimitTypesNotAllowed() {
        //Given
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);

        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler);

        //Then
        assertThrows(IllegalStateException.class, () -> quotaCallback.configure(Map.of(
                StaticQuotaConfig.AVAILABLE_RATIO_PROP, 0.5,
                StaticQuotaConfig.AVAILABLE_BYTES_PROP, 1,
                StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, 10,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"
        )));
    }

    @Test
    void excludedPrincipal() {
        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "foo");
        target.configure(Map.of(StaticQuotaConfig.EXCLUDED_PRINCIPAL_NAME_LIST_PROP, "foo,bar",
                StaticQuotaConfig.PRODUCE_QUOTA_PROP, 1024,
                StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));
        double fooQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, foo, "clientId"));
        assertEquals(Double.MAX_VALUE, fooQuotaLimit);

        KafkaPrincipal baz = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "baz");
        double bazQuotaLimit = target.quotaLimit(ClientQuotaType.PRODUCE, target.quotaMetricTags(ClientQuotaType.PRODUCE, baz, "clientId"));
        assertEquals(1024, bazQuotaLimit);
    }

    @Test
    void shouldScheduleStorageChecker() {
        //Given
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService);

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
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService);

        //When
        target.configure(Map.of(StaticQuotaConfig.STORAGE_CHECK_INTERVAL_PROP, "0", StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        //Then
        verify(scheduledExecutorService, times(0)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldNotScheduleStorageCheckWhenCheckIntervalIsNotProvided() {
        //Given
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService);

        //When
        target.configure(Map.of(StaticQuotaConfig.ADMIN_BOOTSTRAP_SERVER_PROP, "localhost:9092"));

        //Then
        verify(scheduledExecutorService, times(0)).scheduleWithFixedDelay(any(), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void shouldShutdownExecutorOnClose() {
        //Given
        ScheduledExecutorService scheduledExecutorService = mock(ScheduledExecutorService.class);
        StaticQuotaCallback target = new StaticQuotaCallback(volumeSourceBuilder, scheduledExecutorService);
        target.configure(MINIMUM_EXECUTABLE_CONFIG);

        //When
        target.close();

        //Verify
        verify(scheduledExecutorService, times(1)).shutdownNow();
    }

    @Test
    void quotaResetRequiredShouldRespectQuotaType() {
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        VolumeObserver volumeObserver = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected initial state");

        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected state on subsequent call without storage state change");

        //When
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(2))));

        //Then
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.FETCH), "unexpected state on subsequent call after 1st storage state change");

        quotaCallback.close();
    }

    @Test
    void quotaResetRequired() {
        ArgumentCaptor<VolumeObserver> argument = ArgumentCaptor.forClass(VolumeObserver.class);
        when(volumeSourceBuilder.withVolumeObserver(argument.capture())).thenReturn(volumeSourceBuilder);
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(volumeSourceBuilder, backgroundScheduler);
        quotaCallback.configure(MINIMUM_EXECUTABLE_CONFIG);
        VolumeObserver volumeObserver = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(1))));
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(1))));
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        volumeObserver.observeVolumeUsage(success(List.of(newVolume(3))));
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

        SortedMap<MetricName, Metric> group = getMetricGroup("io.strimzi.kafka.quotas.StaticQuotaCallback", "StaticQuotaCallback");

        MetricUtils.assertGaugeMetric(group, "Produce", 15.0);
        MetricUtils.assertGaugeMetric(group, "Fetch", 16.0);
        MetricUtils.assertGaugeMetric(group, "Request", 17.0);

        // the mbean name is part of the public api
        MetricName name = group.firstKey();
        String expectedMbeanName = String.format("io.strimzi.kafka.quotas:type=StaticQuotaCallback,name=%s", name.getName());
        assertEquals(expectedMbeanName, name.getMBeanName(), "unexpected mbean name");
    }
}
