/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.quotas;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class TickingClock extends Clock {
    private Clock baseClock;

    public TickingClock() {
        this.baseClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
    }

    public void tick(Duration tickDuration) {
        this.baseClock = Clock.fixed(baseClock.instant().plus(tickDuration), this.getZone());
    }

    @Override
    public ZoneId getZone() {
        return baseClock.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return baseClock.withZone(zone);
    }

    @Override
    public Instant instant() {
        return baseClock.instant();
    }
}
