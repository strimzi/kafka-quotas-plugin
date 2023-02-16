/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.List;


/**
 * The result of an observation of the volumes in your kafka cluster.
 */
public class VolumeUsageObservation {

    /**
     * @return observed volume usage, this is empty if the status is not SUCCESS
     */
    public Collection<VolumeUsage> getVolumeUsages() {
        return volumeUsages;
    }

    /**
     * @return categorises the result of the observation, either SUCCESS or another status that indicated failure
     */
    public VolumeSourceObservationStatus getStatus() {
        return status;
    }

    /**
     * The outcome of an observation
     */
    enum VolumeSourceObservationStatus {
        SUCCESS,
        SAFETY_TIMEOUT,
        DESCRIBE_CLUSTER_ERROR,
        DESCRIBE_LOG_DIR_ERROR,
        EXCEPTION
    }

    private final Collection<VolumeUsage> volumeUsages;
    private final VolumeSourceObservationStatus status;


    private VolumeUsageObservation(Collection<VolumeUsage> volumeUsages, VolumeSourceObservationStatus status) {
        this.volumeUsages = volumeUsages;
        this.status = status;
    }

    /**
     * @param volumeUsages the observed usages
     * @return a volume usage observation containing volumeUsages and having a SUCCESS status
     */
    public static VolumeUsageObservation success(Collection<VolumeUsage> volumeUsages) {
        return new VolumeUsageObservation(volumeUsages, VolumeSourceObservationStatus.SUCCESS);
    }

    /**
     * @param status the failure status
     * @return a volume usage observation containing volumeUsages and having a non SUCCESS status
     */
    public static VolumeUsageObservation failure(VolumeSourceObservationStatus status) {
        if (status == VolumeSourceObservationStatus.SUCCESS) {
            throw new IllegalArgumentException("success is not a failure");
        }
        return new VolumeUsageObservation(List.of(), status);
    }
}
