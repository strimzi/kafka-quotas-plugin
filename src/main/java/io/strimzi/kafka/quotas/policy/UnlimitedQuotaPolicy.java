/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

/**
 * A quota policy which applies no Limits.
 */
public class UnlimitedQuotaPolicy implements QuotaPolicy {

    /**
     * A singleton instance of the policy
     */
    public static final UnlimitedQuotaPolicy INSTANCE = new UnlimitedQuotaPolicy();

    private UnlimitedQuotaPolicy() {
    }

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        return false;
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        return false;
    }

    /**
     * Constant <code>1.0</code> to apply 100% of the quota
     *
     * @return <code>0</code>
     */
    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        return 1.0;
    }

    /**
     * Constant <code>0</code> for consistency with free space policies rather than the legacy used space policy
     *
     * @return <code>0</code>
     */
    @Override
    public Number getSoftLimit() {
        return 0;
    }

    /**
     * Constant <code>0</code> for consistency with free space policies rather than the legacy used space policy
     *
     * @return <code>0</code>
     */
    @Override
    public Number getHardLimit() {
        return 0;
    }
}
