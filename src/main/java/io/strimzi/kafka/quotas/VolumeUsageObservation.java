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
     *
     * @return it's the throwable
     */
    public Throwable getThrowable() {
        return throwable;
    }

    /**
     * The outcome of an observation
     */
    public enum VolumeSourceObservationStatus {
        /**
         * successfully observed the volumes of the cluster
         */
        SUCCESS,

        /**
         * timed out waiting on the future which is to be completed with the observed volumes from the cluster
         */
        SAFETY_TIMEOUT,

        /**
         * there was an error describing the cluster, it's future completed exceptionally
         */
        DESCRIBE_CLUSTER_ERROR,

        /**
         * there was an error describing the log dirs, it's future completed exceptionally
         */
        DESCRIBE_LOG_DIR_ERROR,

        /**
         * there was an unknown runtime exception while attempting to observe the cluster
         */
        EXCEPTION,

        /**
         * thread interrupted while trying to observe the cluster
         */
        INTERRUPTED,

        /**
         * execution exception while attempting to observe the cluster
         */
        EXECUTION_EXCEPTION
    }

    private final Collection<VolumeUsage> volumeUsages;
    private final VolumeSourceObservationStatus status;
    private final Throwable throwable;


    private VolumeUsageObservation(Collection<VolumeUsage> volumeUsages, VolumeSourceObservationStatus status, Throwable throwable) {
        this.volumeUsages = volumeUsages;
        this.status = status;
        this.throwable = throwable;
    }

    /**
     * @param volumeUsages the observed usages
     * @return a volume usage observation containing volumeUsages and having a SUCCESS status
     */
    public static VolumeUsageObservation success(Collection<VolumeUsage> volumeUsages) {
        return new VolumeUsageObservation(volumeUsages, VolumeSourceObservationStatus.SUCCESS, null);
    }

    /**
     * @param status the failure status
     * @param cause the exception that caused the failure (nullable)
     * @return a volume usage observation containing volumeUsages and having a non SUCCESS status
     */
    public static VolumeUsageObservation failure(VolumeSourceObservationStatus status, Throwable cause) {
        if (status == VolumeSourceObservationStatus.SUCCESS) {
            throw new IllegalArgumentException("success is not a failure");
        }
        return new VolumeUsageObservation(List.of(), status, cause);
    }

    @Override
    public String toString() {
        return "VolumeUsageObservation{" +
                "volumeUsages=" + volumeUsages +
                ", status=" + status +
                '}';
    }
}
