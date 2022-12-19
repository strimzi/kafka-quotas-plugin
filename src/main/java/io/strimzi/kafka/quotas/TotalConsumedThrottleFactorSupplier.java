/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Deprecated
public class TotalConsumedThrottleFactorSupplier implements ThrottleFactorSupplier, Consumer<Collection<Volume>> {

    List<Runnable> listeners = new CopyOnWriteArrayList<>();
    private final long consumedBytesHardLimit;
    private final long consumedBytesSoftLimit;

    private final AtomicLong storageUsed = new AtomicLong(0);

    private volatile Double throttleFactor = 1.0d;

    public TotalConsumedThrottleFactorSupplier(long consumedBytesHardLimit, long consumedBytesSoftLimit) {
        this.consumedBytesHardLimit = consumedBytesHardLimit;
        this.consumedBytesSoftLimit = consumedBytesSoftLimit;
    }

    @Override
    public void addUpdateListener(Runnable listener) {
        listeners.add(listener);
    }

    @Override
    public Double get() {
        return throttleFactor;
    }

    @Override
    public void accept(Collection<Volume> volumes) {
        long totalConsumed = volumes.stream().mapToLong(Volume::getConsumedSpace).sum();
        long oldValue = storageUsed.getAndSet(totalConsumed);
        if (oldValue != totalConsumed) {
            if (totalConsumed >= consumedBytesHardLimit) {
                throttleFactor = 0.0;
            } else if (totalConsumed >= consumedBytesSoftLimit) {
                throttleFactor = 1.0d - (1.0d * (totalConsumed - consumedBytesSoftLimit) / (consumedBytesHardLimit - consumedBytesSoftLimit));
            } else {
                throttleFactor = 1.0d;
            }
            for (Runnable listener : listeners) {
                listener.run();
            }
        }
    }
}
