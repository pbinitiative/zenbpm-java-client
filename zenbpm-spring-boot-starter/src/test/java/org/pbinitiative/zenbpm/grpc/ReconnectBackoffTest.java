package org.pbinitiative.zenbpm.grpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReconnectBackoffTest {

    @Test
    void doublesTheDelayUpToTheConfiguredMaximum() {
        AtomicLong clock = new AtomicLong();
        ReconnectBackoff backoff = backoffWithMedianJitter(clock);

        assertEquals(100L, backoff.nextDelay(null));
        assertEquals(200L, backoff.nextDelay(null));
        assertEquals(360L, backoff.nextDelay(null));
        assertEquals(360L, backoff.nextDelay(null));
    }

    @Test
    void resetsTheDelayAfterAStableStream() {
        AtomicLong clock = new AtomicLong();
        ReconnectBackoff backoff = backoffWithMedianJitter(clock);
        long readyAt = backoff.now();

        assertEquals(100L, backoff.nextDelay(readyAt));
        clock.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertEquals(100L, backoff.nextDelay(readyAt));
    }

    @Test
    void doesNotResetAfterALongAttemptThatNeverBecameReady() {
        AtomicLong clock = new AtomicLong();
        ReconnectBackoff backoff = backoffWithMedianJitter(clock);

        assertEquals(100L, backoff.nextDelay(null));
        clock.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertEquals(200L, backoff.nextDelay(null));
    }

    @Test
    void addsTwentyPercentJitterWithoutExceedingTheMaximum() {
        AtomicLong clock = new AtomicLong();
        ReconnectBackoff lowest = new ReconnectBackoff(100L, 400L, 60_000L, clock::get, () -> 0D);
        ReconnectBackoff highest = new ReconnectBackoff(100L, 400L, 60_000L, clock::get, () -> 1D);

        assertEquals(80L, lowest.nextDelay(null));
        assertEquals(120L, highest.nextDelay(null));
        assertEquals(240L, highest.nextDelay(null));
        assertEquals(400L, highest.nextDelay(null));
    }

    private static ReconnectBackoff backoffWithMedianJitter(AtomicLong clock) {
        return new ReconnectBackoff(100L, 400L, 60_000L, clock::get, () -> 0.5D);
    }
}
