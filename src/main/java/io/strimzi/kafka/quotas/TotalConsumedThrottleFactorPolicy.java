/*
 * Copyright 2020, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import static io.strimzi.kafka.quotas.StaticQuotaCallback.metricName;

/**
 * Backwards compatible ThrottleFactorSupplier which calculates the total aggregate volume usage and compares that against soft and hard limits.
 * <p>
 * It will progressively increase the throttle factor as the usage grows between the hard and soft limits. Once the usage is greater than or equal to the hard limit the throttle factor will be {@code 0}.
 */
@Deprecated
public class TotalConsumedThrottleFactorPolicy implements ThrottleFactorPolicy {
    private final long consumedBytesHardLimit;
    private final long consumedBytesSoftLimit;

    private final AtomicLong storageUsed = new AtomicLong(0);
    private final double softLimitRange;

    /**
     * Configures the throttle calculation with both a soft and hard limit.
     * No validation of the limits is performed.
     *
     * @param consumedBytesHardLimit the total number of bytes once reached (or exceeded) the full throttle should apply.
     * @param consumedBytesSoftLimit the total number of bytes once passed the throttling should apply.
     */
    public TotalConsumedThrottleFactorPolicy(long consumedBytesHardLimit, long consumedBytesSoftLimit) {
        this.consumedBytesHardLimit = consumedBytesHardLimit;
        this.consumedBytesSoftLimit = consumedBytesSoftLimit;

        Metrics.newGauge(metricName("TotalStorageUsedBytes", "StorageChecker", "io.strimzi.kafka.quotas"), new Gauge<Long>() {
            public Long value() {
                return storageUsed.get();
            }
        });
        Metrics.newGauge(metricName("SoftLimitBytes", "StorageChecker", "io.strimzi.kafka.quotas"), new StaticLongGauge(consumedBytesSoftLimit));
        Metrics.newGauge(metricName("HardLimitBytes", "StorageChecker", "io.strimzi.kafka.quotas"), new StaticLongGauge(consumedBytesHardLimit));
        softLimitRange = this.consumedBytesHardLimit - this.consumedBytesSoftLimit;
    }

    /**
     * @param volumes updated Volume usage data
     */
    @Override
    public double calculateFactor(Collection<VolumeUsage> volumes) {
        long totalConsumed = volumes.stream().mapToLong(VolumeUsage::getConsumedSpace).sum();
        storageUsed.set(totalConsumed);
        //Ensure we test for any hard limit breach explicitly before checking for any other condition
        if (totalConsumed >= consumedBytesHardLimit) {
            return 0.0;
        } else if (totalConsumed >= consumedBytesSoftLimit) {
            final double breachedAmount = totalConsumed - consumedBytesSoftLimit;
            //We can't get a divide by zero error here when the soft limit is bigger than the hard limit as we test the hard limit explicitly in the branch above
            return 1.0d - breachedAmount / softLimitRange;
        } else {
            return 1.0d;
        }
    }

    private static class StaticLongGauge extends Gauge<Long> {
        private final Long value;

        private StaticLongGauge(Long value) {
            this.value = value;
        }

        @Override
        public Long value() {
            return value;
        }
    }
}


