/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

public interface QuotaPolicy {

    boolean breachesHardLimit(VolumeDetails volumeDetails);

    boolean breachesSoftLimit(VolumeDetails volumeDetails);

    double quotaFactor(VolumeDetails volumeDetails);

    Number getSoftLimit();

    Number getHardLimit();
}
