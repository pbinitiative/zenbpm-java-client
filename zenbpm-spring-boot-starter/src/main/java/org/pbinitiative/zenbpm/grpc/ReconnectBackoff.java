package org.pbinitiative.zenbpm.grpc;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

final class ReconnectBackoff {

    private final long initialDelayMillis;
    private final long maximumDelayMillis;
    private final long resetAfterNanos;
    private final LongSupplier nanoTime;

    private long currentDelayMillis;

    ReconnectBackoff(
            long initialDelayMillis,
            long maximumDelayMillis,
            long resetAfterMillis,
            LongSupplier nanoTime
    ) {
        if (initialDelayMillis <= 0L) {
            throw new IllegalArgumentException("initialDelayMillis must be positive");
        }
        if (maximumDelayMillis < initialDelayMillis) {
            throw new IllegalArgumentException("maximumDelayMillis must be at least initialDelayMillis");
        }
        if (resetAfterMillis < 0L) {
            throw new IllegalArgumentException("resetAfterMillis must not be negative");
        }
        this.initialDelayMillis = initialDelayMillis;
        this.maximumDelayMillis = maximumDelayMillis;
        this.resetAfterNanos = TimeUnit.MILLISECONDS.toNanos(resetAfterMillis);
        this.nanoTime = nanoTime;
        reset();
    }

    long now() {
        return nanoTime.getAsLong();
    }

    void reset() {
        currentDelayMillis = initialDelayMillis;
    }

    long nextDelay(long streamOpenedAtNanos) {
        if (elapsedSince(streamOpenedAtNanos) >= resetAfterNanos) {
            reset();
        }

        long delayMillis = currentDelayMillis;
        currentDelayMillis = doubledDelay();
        return delayMillis;
    }

    private long elapsedSince(long startNanos) {
        return Math.max(0L, now() - startNanos);
    }

    private long doubledDelay() {
        if (currentDelayMillis > maximumDelayMillis / 2L) {
            return maximumDelayMillis;
        }
        return Math.min(maximumDelayMillis, currentDelayMillis * 2L);
    }
}
