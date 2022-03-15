/*
 * Copyright 2022, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

public class CompositeFreePolicy implements QuotaPolicy {
    private final QuotaPolicy softLimitPolicy;
    private final QuotaPolicy hardLimitPolicy;

    public CompositeFreePolicy(QuotaPolicy softLimitPolicy, QuotaPolicy hardLimitPolicy) {
        this.softLimitPolicy = softLimitPolicy;
        this.hardLimitPolicy = hardLimitPolicy;
    }

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        return hardLimitPolicy.breachesHardLimit(volumeDetails);
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        return softLimitPolicy.breachesSoftLimit(volumeDetails);
    }

    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        //Quota factor only really applies to the period between the soft and hard limits. So we use the soft policy
        return softLimitPolicy.quotaFactor(volumeDetails);
    }

    @Override
    public Number getSoftLimit() {
        return softLimitPolicy.getSoftLimit();
    }

    @Override
    public Number getHardLimit() {
        return hardLimitPolicy.getHardLimit();
    }

    public QuotaPolicy getSoftLimitPolicy() {
        return softLimitPolicy;
    }

    public QuotaPolicy getHardLimitPolicy() {
        return hardLimitPolicy;
    }
}
