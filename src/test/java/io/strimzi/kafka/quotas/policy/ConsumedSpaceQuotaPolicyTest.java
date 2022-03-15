/*
 * Copyright 2022, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumedSpaceQuotaPolicyTest {

    private static final double EPSILON = 0.00001;
    private ConsumedSpaceQuotaPolicy quotaPolicy;

    @BeforeEach
    void setUp() {
        quotaPolicy = new ConsumedSpaceQuotaPolicy(10L, 15L);
    }


    @Test
    void shouldReturnTrueWhenConsumedBytesGreaterThanSoftLimit() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(12L);

        //When
        final boolean breachesSoftLimit = quotaPolicy.breachesSoftLimit(diskOne);

        //Then
        assertTrue(breachesSoftLimit);
    }

    @Test
    void shouldReturnFalseWhenConsumedBytesAreEqualToSoftLimit() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(10L);

        //When
        final boolean breachesSoftLimit = quotaPolicy.breachesSoftLimit(diskOne);

        //Then
        assertFalse(breachesSoftLimit);
    }

    @Test
    void shouldReturnTrueWhenConsumedBytesGreaterThanHardLimit() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(16L);

        //When
        final boolean breachesSoftLimit = quotaPolicy.breachesSoftLimit(diskOne);

        //Then
        assertTrue(breachesSoftLimit);
    }

    @Test
    void shouldReturnTrueWhenConsumedBytesEqualToHardLimit() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(15L);

        //When
        final boolean breachesSoftLimit = quotaPolicy.breachesSoftLimit(diskOne);

        //Then
        assertTrue(breachesSoftLimit);
    }


    @Test
    void shouldReturnQuotaFactorToZeroIfHardLimitBreached() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(20L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.0D, quotaFactor, EPSILON);
    }

    @Test
    void shouldReturnQuotaFactorToZeroIfUsageIsEqualToHardLimit() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(15L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.0D, quotaFactor, EPSILON);
    }

    @Test
    void shouldReduceQuotaFactorIfUsageBetweenSoftAndHardLimits() {
        //Given
        final VolumeDetails diskOne = newVolumeWithConsumedCapacity(12L);

        //When
        final double quotaFactor = quotaPolicy.quotaFactor(diskOne);

        //Then
        assertEquals(0.6D, quotaFactor, EPSILON);
    }

    private VolumeDetails newVolumeWithConsumedCapacity(long consumedCapacity) {
        return new VolumeDetails("Disk One", 20L, consumedCapacity);
    }
}
