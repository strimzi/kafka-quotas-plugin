/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Collection;

/**
 * Defines the interface for being notified of the clusters volume usage.
 */
@FunctionalInterface
public interface VolumeObserver {

    /**
     * @param observedVolumes a collection of VolumeUsage observations
     */
    void observeVolumeUsage(Collection<VolumeUsage> observedVolumes);
}
