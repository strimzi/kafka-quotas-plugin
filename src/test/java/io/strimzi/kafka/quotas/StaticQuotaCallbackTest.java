/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.quota.ClientQuotaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class StaticQuotaCallbackTest {

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

    @Test
    void quotaResetRequired() {
        StorageChecker mock = mock(StorageChecker.class);
        ArgumentCaptor<Consumer<Long>> argument = ArgumentCaptor.forClass(Consumer.class);
        doNothing().when(mock).configure(anyLong(), anyList(), argument.capture());
        StaticQuotaCallback quotaCallback = new StaticQuotaCallback(mock);
        quotaCallback.configure(Map.of());
        Consumer<Long> storageUpdateConsumer = argument.getValue();
        quotaCallback.updateClusterMetadata(null);

        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected initial state");
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        storageUpdateConsumer.accept(1L);
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 1st storage state change");
        storageUpdateConsumer.accept(1L);
        assertFalse(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call without storage state change");
        storageUpdateConsumer.accept(2L);
        assertTrue(quotaCallback.quotaResetRequired(ClientQuotaType.PRODUCE), "unexpected state on subsequent call after 2nd storage state change");

        quotaCallback.close();
    }
}
