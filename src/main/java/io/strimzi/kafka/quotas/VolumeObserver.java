/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

/**
 * Defines the interface for being notified of the clusters volume usage.
 */
@FunctionalInterface
public interface VolumeObserver {

    /**
     * @param result the result of a VolumeUsage observations
     */
    void observeVolumeUsage(VolumeUsageResult result);
}
