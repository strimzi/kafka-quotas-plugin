/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

public class MinFreePercentageQuotaPolicy implements QuotaPolicy {

    private final double softLimitMinFreePercentage;
    private final double hardLimitMinFreePercentage;

    public MinFreePercentageQuotaPolicy(Double softLimitMinFreePercentage, Double hardLimitMinFreePercentage) {
        this.softLimitMinFreePercentage = softLimitMinFreePercentage != null ? softLimitMinFreePercentage : 0.0;
        this.hardLimitMinFreePercentage = hardLimitMinFreePercentage != null ? hardLimitMinFreePercentage : 0.0;
    }

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        final long minFreeBytes = getMinFreeBytesForLimit(volumeDetails, hardLimitMinFreePercentage);
        return volumeDetails.getFreeCapacity() <= minFreeBytes;
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        final long minFreeBytes = getMinFreeBytesForLimit(volumeDetails, softLimitMinFreePercentage);
        return volumeDetails.getFreeCapacity() < minFreeBytes;
    }

    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        final long minFreeBytes = getMinFreeBytesForLimit(volumeDetails, softLimitMinFreePercentage);
        final long overQuotaUsage = volumeDetails.getFreeCapacity() - minFreeBytes;
        final long quotaCapacity = getMinFreeBytesForLimit(volumeDetails, hardLimitMinFreePercentage) - minFreeBytes;
        return Math.max(0.0, 1.0 - (1.0 * overQuotaUsage / quotaCapacity));
    }

    public Number getSoftLimit() {
        return softLimitMinFreePercentage;
    }

    public Number getHardLimit() {
        return hardLimitMinFreePercentage;
    }

    private long getMinFreeBytesForLimit(VolumeDetails volumeDetails, double limit) {
        return Double.valueOf(volumeDetails.getTotalCapacity() * limit).longValue();
    }
}
