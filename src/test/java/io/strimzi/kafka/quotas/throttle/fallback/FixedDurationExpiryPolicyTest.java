/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas.throttle.fallback;

import io.strimzi.kafka.quotas.TickingClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

class FixedDurationExpiryPolicyTest {


    private TickingClock clock;
    private Instant start;
    private FixedDurationExpiryPolicy expiryPolicy;

    @BeforeEach
    void setUp() {
        clock = new TickingClock();
        start = clock.instant();
        expiryPolicy = new FixedDurationExpiryPolicy(clock);
    }

    @Test
    public void shouldNotExpireIfZeroTimeElapsed() {
        clock.tick(Duration.ZERO);
        assertThat(expiryPolicy.isExpired(start)).isFalse();
    }

    @Test
    public void shouldNotExpireBeforeBoundary() {
        clock.tick(Duration.of(5, ChronoUnit.MINUTES).minusNanos(1L));
        assertThat(expiryPolicy.isExpired(start)).isFalse();
    }

    @Test
    public void shouldNotExpireAtBoundary() {
        clock.tick(Duration.of(5, ChronoUnit.MINUTES));
        assertThat(expiryPolicy.isExpired(start)).isFalse();
    }

    @Test
    public void shouldExpireImmediatelyAfterBoundary() {
        clock.tick(Duration.of(5, ChronoUnit.MINUTES).plusNanos(1));
        assertThat(expiryPolicy.isExpired(start)).isTrue();
    }

}