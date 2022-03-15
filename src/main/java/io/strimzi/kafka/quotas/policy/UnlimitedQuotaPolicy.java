/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

public class UnlimitedQuotaPolicy implements QuotaPolicy {

    public static final UnlimitedQuotaPolicy INSTANCE = new UnlimitedQuotaPolicy();

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        return false;
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        return false;
    }

    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        return 1.0;
    }

    @Override
    public Number getSoftLimit() {
        return 0; //Return 0 for consistency with free space policies rather than legacy used space
    }

    @Override
    public Number getHardLimit() {
        return 0; //Return 0 for consistency with free space policies rather than legacy used space
    }
}
