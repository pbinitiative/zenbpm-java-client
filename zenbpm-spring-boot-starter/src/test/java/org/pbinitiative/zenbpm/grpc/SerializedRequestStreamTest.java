package org.pbinitiative.zenbpm.grpc;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.pbinitiative.zenbpm.proto.Zenbpm;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SerializedRequestStreamTest {

    @Test
    void serializesConcurrentWrites() throws Exception {
        AtomicInteger activeWrites = new AtomicInteger();
        AtomicInteger maximumActiveWrites = new AtomicInteger();
        CountDownLatch firstWriteEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstWrite = new CountDownLatch(1);
        SerializedRequestStream stream = new SerializedRequestStream(new StreamObserver<Zenbpm.JobStreamRequest>() {
            @Override
            public void onNext(Zenbpm.JobStreamRequest request) {
                int active = activeWrites.incrementAndGet();
                maximumActiveWrites.updateAndGet(previous -> Math.max(previous, active));
                firstWriteEntered.countDown();
                await(releaseFirstWrite);
                activeWrites.decrementAndGet();
            }

            @Override
            public void onError(Throwable throwable) {
                // No action needed.
            }

            @Override
            public void onCompleted() {
                // No action needed.
            }
        });
        Zenbpm.JobStreamRequest request = Zenbpm.JobStreamRequest.newBuilder().build();
        ExecutorService writers = Executors.newFixedThreadPool(2);

        try {
            Future<Boolean> first = writers.submit(() -> stream.send(request));
            assertTrue(firstWriteEntered.await(5L, TimeUnit.SECONDS));
            Future<Boolean> second = writers.submit(() -> stream.send(request));

            Thread.sleep(100L);
            assertEquals(1, maximumActiveWrites.get());

            releaseFirstWrite.countDown();
            assertTrue(first.get(5L, TimeUnit.SECONDS));
            assertTrue(second.get(5L, TimeUnit.SECONDS));
        } finally {
            releaseFirstWrite.countDown();
            writers.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5L, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
