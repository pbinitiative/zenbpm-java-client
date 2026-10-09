package org.pbinitiative.zenbpm.grpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReconnectBackoffTest {

    @Test
    void doublesTheDelayUpToTheConfiguredMaximum() {
        AtomicLong clock = new AtomicLong();
        ReconnectBackoff backoff = new ReconnectBackoff(100L, 400L, 60_000L, clock::get);

        assertEquals(100L, backoff.nextDelay(backoff.now()));
        assertEquals(200L, backoff.nextDelay(backoff.now()));
        assertEquals(400L, backoff.nextDelay(backoff.now()));
        assertEquals(400L, backoff.nextDelay(backoff.now()));
    }

    @Test
    void resetsTheDelayAfterAStableStream() {
        AtomicLong clock = new AtomicLong();
        ReconnectBackoff backoff = new ReconnectBackoff(100L, 400L, 60_000L, clock::get);
        long openedAt = backoff.now();

        assertEquals(100L, backoff.nextDelay(openedAt));
        clock.addAndGet(TimeUnit.MINUTES.toNanos(1));

        assertEquals(100L, backoff.nextDelay(openedAt));
    }
}
