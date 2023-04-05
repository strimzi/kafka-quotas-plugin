/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Objects;

/**
 * Represents a single volume on a specific Kafka broker.
 */
public class VolumeUsage {
    private final String brokerId;
    private final String logDir;
    private final long capacity;
    private final long availableBytes;

    /**
     * Represents a snapshot of volume usage.
     * @param brokerId which broker does this volume belong too
     * @param logDir the specific logDir the volume hosts
     * @param capacity How many bytes the volume holds
     * @param availableBytes How many available bytes remain on the volume.
     */
    public VolumeUsage(String brokerId, String logDir, long capacity, long availableBytes) {
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.capacity = capacity;
        this.availableBytes = availableBytes;
    }

    /**
     * The source BrokerId.
     * @return The brokerId for the broker holding the volume
     */
    public String getBrokerId() {
        return brokerId;
    }

    /**
     * The log directory, logDir in Kafka parlance.
     * @return the path identifying the logDir on the broker its hosted by.
     */
    public String getLogDir() {
        return logDir;
    }

    /**
     * The capacity of the underlying Volume.
     * @return The size of the volume in bytes.
     */
    public long getCapacity() {
        return capacity;
    }

    /**
     * The available capacity of the underlying volume.
     * @return The number available (free) remaining on the volume.
     */
    public long getAvailableBytes() {
        return availableBytes;
    }

    /**
     * The consumed capacity of the underlying volume.
     * @return The number of bytes on the volume which have been consumed (used).
     */
    public long getConsumedSpace() {
        return capacity - availableBytes;
    }

    /**
     * Expresses the available space as a ratio in the range [0,1].
     * @return The ratio of available bytes to capacity bytes (0.0 if capacity is 0 bytes).
     */
    public double getAvailableRatio() {
        if (capacity == 0) {
            return 0;
        }
        return (double) availableBytes / capacity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VolumeUsage volumeUsage = (VolumeUsage) o;
        return capacity == volumeUsage.capacity && availableBytes == volumeUsage.availableBytes && Objects.equals(brokerId, volumeUsage.brokerId) && Objects.equals(logDir, volumeUsage.logDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, logDir, capacity, availableBytes);
    }

    @Override
    public String toString() {
        return "Volume{" +
                "brokerId='" + brokerId + '\'' +
                ", name='" + logDir + '\'' +
                ", capacity=" + capacity +
                ", usableBytes=" + availableBytes +
                '}';
    }
}
