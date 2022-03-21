/*
 * Copyright 2022, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

/**
 * A quota policy which applies its limits once the consumed disk space passes a configured level.
 */
public class ConsumedSpaceQuotaPolicy implements QuotaPolicy {

    private final long softLimitUsedBytes;

    private final long hardLimitUsedBytes;

    /**
     * Use <code>Long.MAX_VALUE</code> to signal no limit
     * @param softLimitUsedBytes The number of bytes after which the policy starts throttling.
     * @param hardLimitUsedBytes The number of bytes after which the policy is fully throttled.
     */
    public ConsumedSpaceQuotaPolicy(long softLimitUsedBytes, long hardLimitUsedBytes) {
        this.softLimitUsedBytes = softLimitUsedBytes;
        this.hardLimitUsedBytes = hardLimitUsedBytes;
    }

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        return volumeDetails.getConsumedCapacity() >= hardLimitUsedBytes;
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        return volumeDetails.getConsumedCapacity() > softLimitUsedBytes;
    }

    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        final long overQuotaUsage = volumeDetails.getConsumedCapacity() - softLimitUsedBytes;
        final long quotaCapacity = hardLimitUsedBytes - softLimitUsedBytes;
        return Math.max(0.0, 1.0 - (1.0 * overQuotaUsage / quotaCapacity));
    }

    @Override
    public Number getSoftLimit() {
        return softLimitUsedBytes;
    }

    @Override
    public Number getHardLimit() {
        return hardLimitUsedBytes;
    }
}
