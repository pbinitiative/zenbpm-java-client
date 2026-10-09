package org.pbinitiative.zenbpm.grpc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

final class ReconnectBackoff {

    private static final double JITTER_FACTOR = 0.2D;

    private final long initialDelayMillis;
    private final long maximumDelayMillis;
    private final long resetAfterNanos;
    private final LongSupplier nanoTime;
    private final DoubleSupplier random;

    private long currentDelayMillis;

    ReconnectBackoff(
            long initialDelayMillis,
            long maximumDelayMillis,
            long resetAfterMillis,
            LongSupplier nanoTime
    ) {
        this(
                initialDelayMillis,
                maximumDelayMillis,
                resetAfterMillis,
                nanoTime,
                () -> ThreadLocalRandom.current().nextDouble());
    }

    ReconnectBackoff(
            long initialDelayMillis,
            long maximumDelayMillis,
            long resetAfterMillis,
            LongSupplier nanoTime,
            DoubleSupplier random
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
        this.random = random;
        reset();
    }

    long now() {
        return nanoTime.getAsLong();
    }

    void reset() {
        currentDelayMillis = initialDelayMillis;
    }

    long nextDelay(Long streamReadyAtNanos) {
        if (streamReadyAtNanos != null && elapsedSince(streamReadyAtNanos) >= resetAfterNanos) {
            reset();
        }

        long delayMillis = jittered(currentDelayMillis);
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

    private long jittered(long delayMillis) {
        double boundedRandom = Math.max(0D, Math.min(1D, random.getAsDouble()));
        long minimum = Math.max(1L, Math.round(delayMillis * (1D - JITTER_FACTOR)));
        long maximum = Math.min(
                maximumDelayMillis,
                Math.round(delayMillis * (1D + JITTER_FACTOR)));
        return Math.round(minimum + ((maximum - minimum) * boundedRandom));
    }
}
