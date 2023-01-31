/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Objects;

/**
 * Represents a single volume on a specific Kafka broker.
 */
public class Volume {
    private final String brokerId;
    private final String logDir;
    private final long capacity;
    private final long availableBytes;

    /**
     *
     * @param brokerId which broker does this volume belong too
     * @param logDir the specific logDir the volume hosts
     * @param capacity How many bytes the volume holds
     * @param availableBytes How many available bytes remain on the volume.
     */
    public Volume(String brokerId, String logDir, long capacity, long availableBytes) {
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.capacity = capacity;
        this.availableBytes = availableBytes;
    }

    /**
     *
     * @return The brokerId for the broker holding the volume
     */
    public String getBrokerId() {
        return brokerId;
    }

    /**
     *
     * @return the path identifying the logDir on the broker its hosted by.
     */
    public String getLogDir() {
        return logDir;
    }

    /**
     * @return The size of the volume in bytes.
     */
    public long getCapacity() {
        return capacity;
    }

    /**
     *
     * @return The number available (free) remaining on the volume.
     */
    public long getAvailableBytes() {
        return availableBytes;
    }

    /**
     *
     * @return The number of bytes on the volume which have been consumed (used).
     */
    public long getConsumedSpace() {
        return capacity - availableBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Volume volume = (Volume) o;
        return capacity == volume.capacity && availableBytes == volume.availableBytes && Objects.equals(brokerId, volume.brokerId) && Objects.equals(logDir, volume.logDir);
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
