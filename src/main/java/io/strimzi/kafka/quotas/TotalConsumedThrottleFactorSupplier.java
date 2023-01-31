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

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.metricName;

/**
 * Backwards compatible ThrottleFactorSupplier which calculates the total aggregate volume usage and compares that against soft and hard limits.
 * <p>
 * It will progressively increase the throttle factor as the usage grows between the hard and soft limits. Once the usage is greater than or equal to the hard limit the throttle factor will be {@code 0}.
 */
@Deprecated
public class TotalConsumedThrottleFactorSupplier implements ThrottleFactorSupplier, Consumer<Collection<Volume>> {

    List<Runnable> listeners = new CopyOnWriteArrayList<>();
    private final long consumedBytesHardLimit;
    private final long consumedBytesSoftLimit;

    private final AtomicLong storageUsed = new AtomicLong(0);

    private volatile Double throttleFactor = 1.0d;

    /**
     * Configures the throttle calculation with both a soft and hard limit.
     * No validation of the limits is performed.
     * @param consumedBytesHardLimit the total number of bytes once reached (or exceeded) the full throttle should apply.
     * @param consumedBytesSoftLimit the total number of bytes once passed the throttling should apply.
     */
    public TotalConsumedThrottleFactorSupplier(long consumedBytesHardLimit, long consumedBytesSoftLimit) {
        this.consumedBytesHardLimit = consumedBytesHardLimit;
        this.consumedBytesSoftLimit = consumedBytesSoftLimit;

        Metrics.newGauge(metricName("TotalStorageUsedBytes", "StorageChecker", "io.strimzi.kafka.quotas"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        Metrics.newGauge(metricName("SoftLimitBytes", "StorageChecker", "io.strimzi.kafka.quotas"), new Gauge<Long>() {
            public Long value() {
                return consumedBytesSoftLimit;
            }
        });
        Metrics.newGauge(metricName("HardLimitBytes", "StorageChecker", "io.strimzi.kafka.quotas"), new Gauge<Long>() {
            public Long value() {
                return consumedBytesHardLimit;
            }
        });
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
