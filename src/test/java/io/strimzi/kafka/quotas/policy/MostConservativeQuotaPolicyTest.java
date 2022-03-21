/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MostConservativeQuotaPolicyTest {

    private QuotaPolicy minFreeBytesPolicy;
    private QuotaPolicy minFreePercetnagePolicy;
    private MostConservativeQuotaPolicy mostConservativeQuotaPolicy;
    private VolumeDetails testDisk;

    @BeforeEach
    void setUp() {
        minFreeBytesPolicy = mock(QuotaPolicy.class, "MinFreeBytesPolicy");
        minFreePercetnagePolicy = mock(QuotaPolicy.class, "MinFreePercentagePolicy");
        mostConservativeQuotaPolicy = new MostConservativeQuotaPolicy(minFreeBytesPolicy, minFreePercetnagePolicy);

        testDisk = new VolumeDetails("TestDisk", 200L, 100L);
    }

    @Test
    void shouldDelegateToBytesPolicySoftLimit() {
        //Given
        when(minFreeBytesPolicy.breachesSoftLimit(any(VolumeDetails.class))).thenReturn(true);
        when(minFreePercetnagePolicy.breachesSoftLimit(any(VolumeDetails.class))).thenReturn(false);

        //When
        final boolean breachesSoftLimit = mostConservativeQuotaPolicy.breachesSoftLimit(testDisk);

        //Then
        assertTrue(breachesSoftLimit);
        verify(minFreeBytesPolicy).breachesSoftLimit(any(VolumeDetails.class));
    }

    @Test
    void shouldDelegateToPercentagePolicySoftLimit() {
        //Given
        when(minFreeBytesPolicy.breachesSoftLimit(any(VolumeDetails.class))).thenReturn(false);
        when(minFreePercetnagePolicy.breachesSoftLimit(any(VolumeDetails.class))).thenReturn(true);

        //When
        final boolean breachesSoftLimit = mostConservativeQuotaPolicy.breachesSoftLimit(testDisk);

        //Then
        assertTrue(breachesSoftLimit);
        verify(minFreePercetnagePolicy).breachesSoftLimit(any(VolumeDetails.class));
    }
    
    @Test
    void shouldDelegateToBytesPolicyHardLimit() {
        //Given
        when(minFreeBytesPolicy.breachesHardLimit(any(VolumeDetails.class))).thenReturn(true);
        when(minFreePercetnagePolicy.breachesHardLimit(any(VolumeDetails.class))).thenReturn(false);

        //When
        final boolean breachesHardLimit = mostConservativeQuotaPolicy.breachesHardLimit(testDisk);

        //Then
        assertTrue(breachesHardLimit);
        verify(minFreeBytesPolicy).breachesHardLimit(any(VolumeDetails.class));
    }

    @Test
    void shouldDelegateToPercentagePolicyHardLimit() {
        //Given
        when(minFreeBytesPolicy.breachesHardLimit(any(VolumeDetails.class))).thenReturn(false);
        when(minFreePercetnagePolicy.breachesHardLimit(any(VolumeDetails.class))).thenReturn(true);

        //When
        final boolean breachesHardLimit = mostConservativeQuotaPolicy.breachesHardLimit(testDisk);

        //Then
        assertTrue(breachesHardLimit);
        verify(minFreePercetnagePolicy).breachesHardLimit(any(VolumeDetails.class));
    }

    @Test
    void shouldChooseSmallestQuotaFactor() {
        //Given
        when(minFreeBytesPolicy.quotaFactor(any(VolumeDetails.class))).thenReturn(0.2);
        when(minFreePercetnagePolicy.quotaFactor(any(VolumeDetails.class))).thenReturn(0.1);

        //When
        final double quotaFactor = mostConservativeQuotaPolicy.quotaFactor(testDisk);

        //Then
        assertEquals(0.1, quotaFactor);
    }
    
    @Test
    void shouldChooseSmallestSoftLimit() {
        //Given
        when(minFreeBytesPolicy.getSoftLimit()).thenReturn(100L);
        when(minFreePercetnagePolicy.getSoftLimit()).thenReturn(0.1);

        //When
        final Number softLimit = mostConservativeQuotaPolicy.getSoftLimit();

        //Then
        assertEquals(0.1, softLimit);
    }
    
    @Test
    void shouldChooseSmallestHardLimit() {
        //Given
        when(minFreeBytesPolicy.getHardLimit()).thenReturn(100L);
        when(minFreePercetnagePolicy.getHardLimit()).thenReturn(0.1);

        //When
        final Number hardLimit = mostConservativeQuotaPolicy.getHardLimit();

        //Then
        assertEquals(0.1, hardLimit);
    }
    @Test
    void shouldChooseSmallestNonZeroSoftLimit() {
        //Given
        when(minFreeBytesPolicy.getSoftLimit()).thenReturn(100L);
        when(minFreePercetnagePolicy.getSoftLimit()).thenReturn(0.0);

        //When
        final Number softLimit = mostConservativeQuotaPolicy.getSoftLimit();

        //Then
        assertEquals(100.0, softLimit);
    }

    @Test
    void shouldChooseSmallestNonZeroHardLimit() {
        //Given
        when(minFreeBytesPolicy.getHardLimit()).thenReturn(100L);
        when(minFreePercetnagePolicy.getHardLimit()).thenReturn(0.0);

        //When
        final Number hardLimit = mostConservativeQuotaPolicy.getHardLimit();

        //Then
        assertEquals(100.0, hardLimit);
    }
}
