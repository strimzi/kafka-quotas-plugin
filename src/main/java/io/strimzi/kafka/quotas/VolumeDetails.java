/*
 * Copyright 2021, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

public class VolumeDetails {
    private final String volumeName;
    private final long totalCapacity;
    private final long consumedCapacity;

    public VolumeDetails(String volumeName, long totalCapacity, long consumedCapacity) {
        this.volumeName = volumeName;
        this.totalCapacity = totalCapacity;
        this.consumedCapacity = consumedCapacity;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public long getTotalCapacity() {
        return totalCapacity;
    }

    public long getConsumedCapacity() {
        return consumedCapacity;
    }

    public long getFreeCapacity() {
        return totalCapacity - consumedCapacity;
    }
}
