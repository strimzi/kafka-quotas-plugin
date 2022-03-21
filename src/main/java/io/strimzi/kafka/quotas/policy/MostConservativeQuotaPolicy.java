/*
 * Copyright 2022, Red Hat Inc.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.quotas.policy;

import java.util.function.Supplier;

import io.strimzi.kafka.quotas.StaticQuotaCallback;
import io.strimzi.kafka.quotas.VolumeDetails;

public class MostConservativeQuotaPolicy implements QuotaPolicy {

    private final QuotaPolicy freeBytesQuotaPolicy;
    private final QuotaPolicy freePercentageQuotaPolicy;

    public MostConservativeQuotaPolicy(QuotaPolicy freeBytesQuotaPolicy, QuotaPolicy freePercentageQuotaPolicy) {
        this.freeBytesQuotaPolicy = freeBytesQuotaPolicy;
        this.freePercentageQuotaPolicy = freePercentageQuotaPolicy;
    }

    @Override
    public boolean breachesHardLimit(VolumeDetails volumeDetails) {
        final boolean breachesBytesLimit = freeBytesQuotaPolicy.breachesHardLimit(volumeDetails);
        final boolean breachesPercentageLimit = freePercentageQuotaPolicy.breachesHardLimit(volumeDetails);
        return breachesBytesLimit || breachesPercentageLimit;
    }

    @Override
    public boolean breachesSoftLimit(VolumeDetails volumeDetails) {
        final boolean breachesBytesLimit = freeBytesQuotaPolicy.breachesSoftLimit(volumeDetails);
        final boolean breachesPercentageLimit = freePercentageQuotaPolicy.breachesSoftLimit(volumeDetails);
        return breachesBytesLimit || breachesPercentageLimit;
    }

    @Override
    public double quotaFactor(VolumeDetails volumeDetails) {
        final double freeBytesFactor = freeBytesQuotaPolicy.quotaFactor(volumeDetails);
        final double freePercentageFactor = freePercentageQuotaPolicy.quotaFactor(volumeDetails);
        return Math.min(freeBytesFactor, freePercentageFactor);
    }

    @Override
    public Number getSoftLimit() {
        return getLimit(freeBytesQuotaPolicy::getSoftLimit, freePercentageQuotaPolicy::getSoftLimit);
    }

    @Override
    public Number getHardLimit() {
        return getLimit(freeBytesQuotaPolicy::getHardLimit, freePercentageQuotaPolicy::getHardLimit);
    }

    private double getLimit(Supplier<Number> bytesLimitSupplier, Supplier<Number> percentageLimitSupplier) {
        final Number rawByteLimit = bytesLimitSupplier.get();
        final Number rawPercentageLimit = percentageLimitSupplier.get();
        final double bytesLimit = rawByteLimit != null ? rawByteLimit.doubleValue() : 0.0;
        final double percentageLimit = rawPercentageLimit != null ? rawPercentageLimit.doubleValue() : 0.0;
        final boolean hasBytesLimit = hasLimit(bytesLimit);
        final boolean hasPercentageLimit = hasLimit(percentageLimit);
        if (hasBytesLimit && hasPercentageLimit) {
            return Math.min(bytesLimit, percentageLimit);
        } else if (hasBytesLimit) {
            return bytesLimit;
        } else {
            return percentageLimit;
        }
    }

    private boolean hasLimit(double value) {
        return Math.abs(value - 0.0) > StaticQuotaCallback.EPSILON;
    }
}
