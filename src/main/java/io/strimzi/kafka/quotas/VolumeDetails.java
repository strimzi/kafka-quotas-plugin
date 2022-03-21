/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

/**
 * VolumeDetails encapsulates the information need by quota policies to determine if a volume is in breach of the policy.
 */
public class VolumeDetails {
    private final String volumeName;
    private final long totalCapacity;
    private final long consumedCapacity;

    /**
     * Constructs an immutable snapshot of the volume.
     * @param volumeName A common name to distinguish this volume from others.
     * @param totalCapacity The total capacity in <code>bytes</code> of this volume
     * @param consumedCapacity The number of <code>bytes</code> already consumed on this volume.
     */
    public VolumeDetails(String volumeName, long totalCapacity, long consumedCapacity) {
        this.volumeName = volumeName;
        this.totalCapacity = totalCapacity;
        this.consumedCapacity = consumedCapacity;
    }

    /**
     * The identifier for this volume.
     *
     * For example: <code>/dev/sda1</code>
     * @return The volume identifier
     */
    public String getVolumeName() {
        return volumeName;
    }

    /**
     * @return The total capacity in <code>bytes</code> of this volume
     */
    public long getTotalCapacity() {
        return totalCapacity;
    }

    /**
     *
     * @return The number of <code>bytes</code> already consumed on this volume.
     */
    public long getConsumedCapacity() {
        return consumedCapacity;
    }

    /**
     * @return The number of available <code>bytes</code> on this volume.
     */
    public long getFreeCapacity() {
        return totalCapacity - consumedCapacity;
    }

    @Override
    public String toString() {
        return "VolumeDetails{" +
                "volumeName='" + volumeName + '\'' +
                ", totalCapacity=" + totalCapacity +
                ", consumedCapacity=" + consumedCapacity +
                '}';
    }
}
