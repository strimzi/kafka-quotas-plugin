/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Map;

import io.strimzi.kafka.quotas.policy.MinFreeBytesQuotaPolicy;
import io.strimzi.kafka.quotas.policy.MinFreePercentageQuotaPolicy;
import io.strimzi.kafka.quotas.policy.MostConservativeQuotaPolicy;
import io.strimzi.kafka.quotas.policy.QuotaPolicy;
import io.strimzi.kafka.quotas.policy.UnlimitedQuotaPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StaticQuotaConfigTest {
    @Test
    void shouldDefaultToUnlimitedQuotaPolicy() {
        //Given
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(Map.of(), false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertEquals(UnlimitedQuotaPolicy.INSTANCE, quotaPolicy);
    }

    @Test
    void shouldCreateMinFreeBytesQuotaPolicyWithoutHardLimit() {
        //Given
        final Map<String, Object> config = Map.of(StaticQuotaConfig.STORAGE_QUOTA_SOFT_FREE_BYTES_PROP, 15L);
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MinFreeBytesQuotaPolicy.class));
        MinFreeBytesQuotaPolicy minFreeBytesQuotaPolicy = (MinFreeBytesQuotaPolicy) quotaPolicy;
        assertEquals(15L, minFreeBytesQuotaPolicy.getSoftLimit());
        assertEquals(0L, minFreeBytesQuotaPolicy.getHardLimit());
    }

    @Test
    void shouldCreateMinFreeBytesQuotaPolicyWithoutSoftLimit() {
        //Given
        final Map<String, Object> config = Map.of(StaticQuotaConfig.STORAGE_QUOTA_HARD_FREE_BYTES_PROP, 15L);
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MinFreeBytesQuotaPolicy.class));
        MinFreeBytesQuotaPolicy minFreeBytesQuotaPolicy = (MinFreeBytesQuotaPolicy) quotaPolicy;
        assertEquals(0L, minFreeBytesQuotaPolicy.getSoftLimit());
        assertEquals(15L, minFreeBytesQuotaPolicy.getHardLimit());
    }

    @Test
    void shouldCreateMinFreeBytesQuotaPolicyWithBothLimits() {
        //Given
        final Map<String, Object> config = Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_FREE_BYTES_PROP, 10L,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_FREE_BYTES_PROP, 5L);
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MinFreeBytesQuotaPolicy.class));
        MinFreeBytesQuotaPolicy minFreeBytesQuotaPolicy = (MinFreeBytesQuotaPolicy) quotaPolicy;
        assertEquals(10L, minFreeBytesQuotaPolicy.getSoftLimit());
        assertEquals(5L, minFreeBytesQuotaPolicy.getHardLimit());
    }

    @Test
    void shouldCreateMinFreePercentageQuotaPolicyWithoutHardLimit() {
        //Given
        final Map<String, Object> config = Map.of(StaticQuotaConfig.STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP, 0.1);
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MinFreePercentageQuotaPolicy.class));
        MinFreePercentageQuotaPolicy minFreePercentageQuotaPolicy = (MinFreePercentageQuotaPolicy) quotaPolicy;
        assertEquals(0.1, minFreePercentageQuotaPolicy.getSoftLimit());
        assertEquals(0.0, minFreePercentageQuotaPolicy.getHardLimit());
    }

    @Test
    void shouldCreateMinFreePercentageQuotaPolicyWithoutSoftLimit() {
        //Given
        final Map<String, Object> config = Map.of(StaticQuotaConfig.STORAGE_QUOTA_HARD_FREE_PERCENT_PROP, 0.1);
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MinFreePercentageQuotaPolicy.class));
        MinFreePercentageQuotaPolicy minFreePercentageQuotaPolicy = (MinFreePercentageQuotaPolicy) quotaPolicy;
        assertEquals(0.0, minFreePercentageQuotaPolicy.getSoftLimit());
        assertEquals(0.1, minFreePercentageQuotaPolicy.getHardLimit());
    }

    @Test
    void shouldCreateMinFreePercentageQuotaPolicyWithBothLimits() {
        //Given
        final Map<String, Object> config = Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP, 0.1,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_FREE_PERCENT_PROP, 0.01);
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MinFreePercentageQuotaPolicy.class));
        assertEquals(0.1, quotaPolicy.getSoftLimit());
        assertEquals(0.01, quotaPolicy.getHardLimit());
    }

    @Test
    void shouldMoreConservativePolicyIfBytesAndPercentageConfiguredForSoftLimit() {
        //Given
        final Map<String, Object> config = Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_FREE_PERCENT_PROP, 0.02,
                StaticQuotaConfig.STORAGE_QUOTA_SOFT_FREE_BYTES_PROP, 10L
        );
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MostConservativeQuotaPolicy.class));
    }

    @Test
    void shouldMoreConservativePolicyIfBytesAndPercentageConfiguredForHardLimit() {
        //Given
        final Map<String, Object> config = Map.of(
                StaticQuotaConfig.STORAGE_QUOTA_HARD_FREE_PERCENT_PROP, 0.01,
                StaticQuotaConfig.STORAGE_QUOTA_HARD_FREE_BYTES_PROP, 5L
        );
        final StaticQuotaConfig staticQuotaConfig = new StaticQuotaConfig(config, false);

        //When
        final QuotaPolicy quotaPolicy = staticQuotaConfig.getQuotaPolicy();

        //Then
        assertTrue(quotaPolicy.getClass().isAssignableFrom(MostConservativeQuotaPolicy.class));
    }
}
