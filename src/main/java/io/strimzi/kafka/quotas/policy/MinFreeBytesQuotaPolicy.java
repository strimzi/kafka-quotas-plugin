/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

public class MinFreeBytesQuotaPolicy implements QuotaPolicy {

    private final long softLimitMinFreeBytes;
    private final long hardLimitMinFreeBytes;

    public MinFreeBytesQuotaPolicy(Long softLimitMinFreeBytes, Long hardLimitMinFreeBytes) {
        this.softLimitMinFreeBytes = softLimitMinFreeBytes != null ? softLimitMinFreeBytes : 0L;
        this.hardLimitMinFreeBytes = hardLimitMinFreeBytes != null ? hardLimitMinFreeBytes : 0L;
    }

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        return volumeDetails.getFreeCapacity() <= hardLimitMinFreeBytes;
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        return volumeDetails.getFreeCapacity() < softLimitMinFreeBytes;
    }

    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        final long overQuotaUsage = volumeDetails.getFreeCapacity() - softLimitMinFreeBytes;
        final long quotaCapacity = hardLimitMinFreeBytes - softLimitMinFreeBytes;
        return Math.max(0.0, 1.0 - (1.0 * overQuotaUsage / quotaCapacity));
    }

    public Number getSoftLimit() {
        return softLimitMinFreeBytes;
    }

    public Number getHardLimit() {
        return hardLimitMinFreeBytes;
    }
}
