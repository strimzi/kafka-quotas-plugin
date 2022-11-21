/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas;

import java.util.Objects;

public class Volume {
    private final String brokerId;
    private final String logDir;
    private final long capacity;
    private final long usableBytes;

    public Volume(String brokerId, String logDir, long capacity, long usableBytes) {
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.capacity = capacity;
        this.usableBytes = usableBytes;
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getLogDir() {
        return logDir;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getConsumedSpace() {
        return capacity - usableBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Volume volume = (Volume) o;
        return capacity == volume.capacity && usableBytes == volume.usableBytes && Objects.equals(brokerId, volume.brokerId) && Objects.equals(logDir, volume.logDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, logDir, capacity, usableBytes);
    }

    @Override
    public String toString() {
        return "Volume{" +
                "brokerId='" + brokerId + '\'' +
                ", name='" + logDir + '\'' +
                ", capacity=" + capacity +
                ", usableBytes=" + usableBytes +
                '}';
    }
}
