/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.policy;

import io.strimzi.kafka.quotas.VolumeDetails;

/**
 * Abstracts the decision-making around hard and soft limits and how to calculate the affect the impact breaching the limits has on the client request.
 */
public interface QuotaPolicy {

    /**
     * Does the particular volume breach this policy's defined hard limit.
     *
     * @param volumeDetails details of the disk in question.
     * @return <code>true</code> if this policy considers the volume to breach the limit otherwise <code>false</code>
     */
    boolean breachesHardLimit(VolumeDetails volumeDetails);

    /**
     * Does the particular volume breach this policy's defined soft limit.
     *
     * @param volumeDetails details of the disk in question.
     * @return <code>true</code> if this policy considers the volume to breach the limit otherwise <code>false</code>
     */
    boolean breachesSoftLimit(VolumeDetails volumeDetails);

    /**
     * Returns the fraction of the original quota this policy thinks is appropriate. Represented as percentage value between <code>0</code> and <code>1</code>
     * <p>
     * Where a fraction of <code>1.0</code> is un affected <br>
     * Breaching the hard limit implies a quota factor of <code>0.0</code>
     * @param volumeDetails details of the disk in question.
     * @return A value between <code>0</code> and <code>1</code>.
     */
    double quotaFactor(VolumeDetails volumeDetails);

    /**
     * At what level does this policy start applying a non-zero quota factor.
     * Primarily for metrics purposes.
     * Note: Returns <code>Number</code> to represent both fixed or relative usage levels. e.g. 5% free
     *
     * @return the level at which the quotaFactor becomes non-zero.
     */
    Number getSoftLimit();

    /**
     * At what level does this policy apply its maximum level of throttling.
     * Primarily for metrics purposes.
     * Note: Returns <code>Number</code> to represent both fixed or relative usage levels. e.g. 5% free
     *
     * @return the level at which the quotaFactor becomes non-zero.
     */
    Number getHardLimit();
}
