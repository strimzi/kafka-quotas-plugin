/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;


/**
 * <p>The result of an observation of the volumes in your kafka cluster.</p>
 * The result can either be:
 * <ul>
 * <li><strong>successful</strong>; status is SUCCESS and volumeUsages is derived from a successful observation of the whole cluster</li>
 * <li><strong>failed</strong>; status is not SUCCESS and volumeUsages is empty</li>
 * </ul>
 */
public class VolumeUsageResult {

    /**
     * The observed usage on a per-volume basis.
     * @return observed volume usage, this is empty if the status is not SUCCESS
     */
    public Collection<VolumeUsage> getVolumeUsages() {
        return Collections.unmodifiableCollection(volumeUsages);
    }

    /**
     * The status of this observation.
     * @return categorises the result of the observation, either SUCCESS or another status that indicated failure
     */
    public VolumeSourceObservationStatus getStatus() {
        return status;
    }

    /**
     * The most recent value for when the log dirs were observed
     * @return An instant marking the most recent observation of any log dir.
     */
    public Instant getObservedAt() {
        return volumeUsages.stream().map(VolumeUsage::getObservedAt).max(Comparator.naturalOrder()).orElseThrow();
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
         * there was an error describing the cluster, its future completed exceptionally
         */
        DESCRIBE_CLUSTER_ERROR,

        /**
         * there was an error describing the log dirs, its future completed exceptionally
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
    private final Class<? extends Throwable> causeOfFailure;


    private VolumeUsageResult(Collection<VolumeUsage> volumeUsages, VolumeSourceObservationStatus status, Class<? extends Throwable> causeOfFailure) {
        this.volumeUsages = volumeUsages;
        this.status = status;
        this.causeOfFailure = causeOfFailure;
    }

    /**
     * Creates an instance to represent a successful observation
     * @param volumeUsages the observed usages
     * @return a volume usage observation containing volumeUsages and having a SUCCESS status
     */
    public static VolumeUsageResult success(Collection<VolumeUsage> volumeUsages) {
        return new VolumeUsageResult(volumeUsages, VolumeSourceObservationStatus.SUCCESS, null);
    }

    /**
     * Creates an instance to represent a failed observation.
     * @param status the failure status
     * @param cause the exception that caused the failure (nullable)
     * @return a volume usage observation containing volumeUsages and having a non SUCCESS status
     */
    public static VolumeUsageResult failure(VolumeSourceObservationStatus status, Class<? extends Throwable> cause) {
        if (status == VolumeSourceObservationStatus.SUCCESS) {
            throw new IllegalArgumentException("success is not a failure");
        }
        return new VolumeUsageResult(List.of(), status, cause);
    }

    /**
     * Clone the original observation and replace the set of observations with the replacements.
     * @param original the VolumeUsageResult to be copied
     * @param replacement the replacement set of observations
     * @return The merged VolumeUsageResult
     */
    public static VolumeUsageResult replaceObservations(VolumeUsageResult original, Collection<VolumeUsage> replacement) {
        return new VolumeUsageResult(replacement, original.status, original.causeOfFailure);
    }

    @Override
    public String toString() {
        return "VolumeUsageObservation{" +
                "volumeUsages=" + volumeUsages +
                ", status=" + status +
                ", throwable=" + causeOfFailure +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VolumeUsageResult that = (VolumeUsageResult) o;
        return Objects.equals(volumeUsages, that.volumeUsages) && status == that.status && Objects.equals(causeOfFailure, that.causeOfFailure);
    }

    @Override
    public int hashCode() {
        return Objects.hash(volumeUsages, status, causeOfFailure);
    }
}
