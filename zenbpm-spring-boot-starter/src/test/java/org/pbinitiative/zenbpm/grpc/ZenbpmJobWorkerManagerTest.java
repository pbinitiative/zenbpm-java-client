package org.pbinitiative.zenbpm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;
import org.pbinitiative.zenbpm.proto.ZenBpmGrpc;
import org.pbinitiative.zenbpm.proto.Zenbpm;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZenbpmJobWorkerManagerTest {

    private Server server;
    private ZenbpmJobWorkerManager manager;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (manager != null) {
            manager.stop();
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void reconnectsAndResubscribesAfterServerRestart() throws Exception {
        RecordingJobService firstService = new RecordingJobService();
        server = startServer(0, firstService);
        int port = server.getPort();

        CountDownLatch handled = new CountDownLatch(1);
        manager = createManager(port, new TestWorker(handled), new SecondTestWorker());
        manager.start();

        assertTrue(waitUntil(() -> firstService.subscriptionCount("test-job") == 1
                        && firstService.subscriptionCount("second-test-job") == 1, 5_000L),
                "worker should subscribe every job type on the initial stream");

        server.shutdownNow();
        assertTrue(server.awaitTermination(5, TimeUnit.SECONDS),
                "initial server should stop");

        RecordingJobService restartedService = new RecordingJobService();
        server = startServer(port, restartedService);

        assertTrue(waitUntil(() -> restartedService.subscriptionCount("test-job") == 1
                        && restartedService.subscriptionCount("second-test-job") == 1, 10_000L),
                "worker should reconnect and restore every subscription");

        restartedService.sendJob();
        assertTrue(handled.await(5, TimeUnit.SECONDS),
                "reconnected worker should dispatch jobs");
        assertTrue(restartedService.completed.await(5, TimeUnit.SECONDS),
                "reconnected worker should complete jobs on the new stream");
    }

    @Test
    void retriesUntilJobStreamBecomesAvailable() throws Exception {
        RecoveringJobService service = new RecoveringJobService();
        server = startServer(0, service);
        CountDownLatch handled = new CountDownLatch(1);
        manager = createManager(server.getPort(), new TestWorker(handled));
        manager.start();

        assertTrue(waitUntil(() -> service.streamCount.get() >= 2, 5_000L),
                "worker should retry while the job stream is unavailable");
        service.available.set(true);

        assertTrue(service.subscribed.await(5, TimeUnit.SECONDS),
                "worker should keep retrying until the server becomes available");

        service.sendJob();
        assertTrue(handled.await(5, TimeUnit.SECONDS),
                "worker connected after startup should dispatch jobs");
        assertTrue(service.completed.await(5, TimeUnit.SECONDS),
                "worker connected after startup should complete jobs");
    }

    @Test
    void reconnectsWhenServerCompletesTheStream() throws Exception {
        RecordingJobService service = new RecordingJobService();
        server = startServer(0, service);
        manager = createManager(server.getPort(), new TestWorker(new CountDownLatch(0)));
        manager.start();

        assertTrue(waitUntil(() -> service.subscriptionCount("test-job") == 1, 5_000L));
        service.completeStream();

        assertTrue(waitUntil(() -> service.subscriptionCount("test-job") == 2, 5_000L),
                "worker should reconnect after server onCompleted");
        assertEquals(2, service.streamCount.get(), "only one replacement stream should be opened");
    }

    @Test
    void reconnectsWhileAHandlerFromTheOldStreamIsBlocked() throws Exception {
        RecordingJobService firstService = new RecordingJobService();
        server = startServer(0, firstService);
        int port = server.getPort();
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        manager = createManager(port, new BlockingWorker(handlerStarted, releaseHandler));
        manager.start();

        assertTrue(waitUntil(() -> firstService.subscriptionCount("test-job") == 1, 5_000L));
        firstService.sendJob();
        assertTrue(handlerStarted.await(5, TimeUnit.SECONDS), "handler should start");

        server.shutdownNow();
        assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
        RecordingJobService restartedService = new RecordingJobService();
        server = startServer(port, restartedService);

        assertTrue(waitUntil(() -> restartedService.subscriptionCount("test-job") == 1, 5_000L),
                "reconnect must not wait for the old handler");
        releaseHandler.countDown();
        assertFalse(restartedService.completed.await(300, TimeUnit.MILLISECONDS),
                "an old job result must not be sent on the replacement stream");
    }

    @Test
    void appliesBackpressureWithoutDroppingBurstJobs() throws Exception {
        int jobCount = 150;
        BurstJobService service = new BurstJobService(jobCount);
        server = startServer(0, service);
        CountDownLatch firstJobStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstJob = new CountDownLatch(1);
        AtomicInteger handledJobs = new AtomicInteger();
        manager = createManager(
                server.getPort(),
                new BurstWorker(firstJobStarted, releaseFirstJob, handledJobs));
        manager.start();

        assertTrue(service.subscribed.await(5, TimeUnit.SECONDS));

        try {
            service.sendJobs(jobCount);
            assertTrue(firstJobStarted.await(5, TimeUnit.SECONDS));
            Thread.sleep(200L);
            assertEquals(1, handledJobs.get(), "only the requested job should reach the handler");
        } finally {
            releaseFirstJob.countDown();
        }

        assertTrue(service.completed.await(10, TimeUnit.SECONDS),
                "every streamed job should be handled and completed");
        assertEquals(jobCount, handledJobs.get());
    }

    @Test
    void growsBackoffWhenAStreamRespondsAndThenFails() throws Exception {
        FlappingJobService service = new FlappingJobService(4);
        server = startServer(0, service);
        manager = createManagerWithBackoff(
                server.getPort(),
                30L,
                120L,
                1_000L,
                new TestWorker(new CountDownLatch(0)));
        manager.start();

        assertTrue(service.streamsOpened.await(5, TimeUnit.SECONDS), "worker should keep reconnecting");
        List<Long> openedAt = service.openedAtNanos;
        long firstToFourthMillis = TimeUnit.NANOSECONDS.toMillis(openedAt.get(3) - openedAt.get(0));
        assertTrue(firstToFourthMillis >= 150L,
                "error responses must not reset reconnect backoff; elapsed=" + firstToFourthMillis + " ms");
    }

    @Test
    void resetsGrpcChannelBackoffBeforeEveryStreamAttempt() throws Exception {
        RecordingJobService service = new RecordingJobService();
        server = startServer(0, service);
        RecordingManagedChannel channel = new RecordingManagedChannel(ManagedChannelBuilder
                .forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build());
        manager = createManager(channel, new TestWorker(new CountDownLatch(0)));
        manager.start();

        assertTrue(waitUntil(() -> service.subscriptionCount("test-job") == 1, 5_000L));
        service.completeStream();
        assertTrue(waitUntil(() -> service.subscriptionCount("test-job") == 2, 5_000L));
        assertEquals(2, channel.resetBackoffCalls.get(),
                "each application reconnect attempt should reset the channel backoff");
    }

    @Test
    void stopCancelsAPendingReconnect() throws Exception {
        RecoveringJobService service = new RecoveringJobService();
        server = startServer(0, service);
        manager = createManagerWithBackoff(
                server.getPort(),
                500L,
                500L,
                1_000L,
                new TestWorker(new CountDownLatch(0)));
        manager.start();

        assertTrue(waitUntil(() -> service.streamCount.get() == 1, 5_000L));
        manager.stop();
        int streamsAtStop = service.streamCount.get();
        Thread.sleep(700L);

        assertEquals(streamsAtStop, service.streamCount.get(), "stop should cancel the scheduled reconnect");
        assertFalse(manager.isRunning());
    }

    private Server startServer(int port, BindableService service) throws IOException {
        return NettyServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();
    }

    private ZenbpmJobWorkerManager createManager(int port, Object... workers) {
        ZenbpmClientProperties properties = properties(port);
        ZenbpmJobWorkerManager workerManager = new ZenbpmJobWorkerManager(
                properties,
                openTelemetryProvider(),
                true);
        registerWorkers(workerManager, workers);
        return workerManager;
    }

    private ZenbpmJobWorkerManager createManagerWithBackoff(
            int port,
            long initialDelayMillis,
            long maximumDelayMillis,
            long resetAfterMillis,
            Object... workers
    ) {
        ZenbpmClientProperties properties = properties(port);
        ZenbpmJobWorkerManager workerManager = new ZenbpmJobWorkerManager(
                properties,
                new JobDispatcher(properties, openTelemetryProvider(), true),
                new DefaultManagedChannelFactory(),
                new ReconnectBackoff(
                        initialDelayMillis,
                        maximumDelayMillis,
                        resetAfterMillis,
                        System::nanoTime));
        registerWorkers(workerManager, workers);
        return workerManager;
    }

    private ZenbpmJobWorkerManager createManager(ManagedChannel channel, Object... workers) {
        ZenbpmClientProperties properties = properties(0);
        ZenbpmJobWorkerManager workerManager = new ZenbpmJobWorkerManager(
                properties,
                new JobDispatcher(properties, openTelemetryProvider(), true),
                ignored -> channel,
                new ReconnectBackoff(30L, 120L, 1_000L, System::nanoTime));
        registerWorkers(workerManager, workers);
        return workerManager;
    }

    private static ZenbpmClientProperties properties(int port) {
        ZenbpmClientProperties properties = new ZenbpmClientProperties();
        properties.setGrpcHost("127.0.0.1");
        properties.setGrpcPort(port);
        properties.setGrpcPlaintext(true);
        properties.setGrpcLoggingEnabled(false);
        properties.setJobWorkerEnabled(true);
        return properties;
    }

    private static ObjectProvider<OpenTelemetry> openTelemetryProvider() {
        return new DefaultListableBeanFactory().getBeanProvider(OpenTelemetry.class);
    }

    private void registerWorkers(ZenbpmJobWorkerManager workerManager, Object... workers) {
        for (int index = 0; index < workers.length; index++) {
            workerManager.postProcessAfterInitialization(workers[index], "testWorker" + index);
        }
    }

    private static boolean waitUntil(BooleanSupplier condition, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10L);
        }
        return condition.getAsBoolean();
    }

    private static class TestWorker {
        private final CountDownLatch handled;

        private TestWorker(CountDownLatch handled) {
            this.handled = handled;
        }

        @JobWorker("test-job")
        public Map<String, Object> handle() {
            handled.countDown();
            return Collections.<String, Object>singletonMap("result", "completed");
        }
    }

    private static class SecondTestWorker {
        @JobWorker("second-test-job")
        public void handle() {
            // Subscription-only worker used to verify that every job type is restored.
        }
    }

    private static class BlockingWorker {
        private final CountDownLatch started;
        private final CountDownLatch release;

        private BlockingWorker(CountDownLatch started, CountDownLatch release) {
            this.started = started;
            this.release = release;
        }

        @JobWorker("test-job")
        public Map<String, Object> handle() throws InterruptedException {
            started.countDown();
            release.await();
            return Collections.<String, Object>singletonMap("result", "completed");
        }
    }

    private static class BurstWorker {
        private final CountDownLatch firstJobStarted;
        private final CountDownLatch releaseFirstJob;
        private final AtomicInteger handledJobs;

        private BurstWorker(
                CountDownLatch firstJobStarted,
                CountDownLatch releaseFirstJob,
                AtomicInteger handledJobs
        ) {
            this.firstJobStarted = firstJobStarted;
            this.releaseFirstJob = releaseFirstJob;
            this.handledJobs = handledJobs;
        }

        @JobWorker("test-job")
        public Map<String, Object> handle(Zenbpm.WaitingJob job) throws InterruptedException {
            handledJobs.incrementAndGet();
            if (job.getKey() == 1L) {
                firstJobStarted.countDown();
                releaseFirstJob.await();
            }
            return Collections.<String, Object>singletonMap("result", "completed");
        }
    }

    private static class BurstJobService extends ZenBpmGrpc.ZenBpmImplBase {
        private final CountDownLatch subscribed = new CountDownLatch(1);
        private final CountDownLatch completed;
        private final AtomicReference<StreamObserver<Zenbpm.JobStreamResponse>> responseObserver =
                new AtomicReference<>();

        private BurstJobService(int expectedCompletions) {
            completed = new CountDownLatch(expectedCompletions);
        }

        @Override
        public StreamObserver<Zenbpm.JobStreamRequest> jobStream(
                StreamObserver<Zenbpm.JobStreamResponse> observer
        ) {
            responseObserver.set(observer);
            return new NoopRequestObserver() {
                @Override
                public void onNext(Zenbpm.JobStreamRequest request) {
                    if (request.hasSubscription()) {
                        subscribed.countDown();
                    } else if (request.hasComplete()) {
                        completed.countDown();
                    }
                }

                @Override
                public void onCompleted() {
                    observer.onCompleted();
                }
            };
        }

        private void sendJobs(int count) {
            for (int index = 1; index <= count; index++) {
                Zenbpm.WaitingJob job = Zenbpm.WaitingJob.newBuilder()
                        .setKey(index)
                        .setType("test-job")
                        .setInputVariables(ByteString.copyFromUtf8("{}"))
                        .build();
                responseObserver.get().onNext(Zenbpm.JobStreamResponse.newBuilder().setJob(job).build());
            }
        }
    }

    private static class RecordingJobService extends ZenBpmGrpc.ZenBpmImplBase {
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicInteger streamCount = new AtomicInteger();
        private final Map<String, AtomicInteger> subscriptionCounts = new ConcurrentHashMap<>();
        private final AtomicReference<StreamObserver<Zenbpm.JobStreamResponse>> responseObserver =
                new AtomicReference<>();

        @Override
        public StreamObserver<Zenbpm.JobStreamRequest> jobStream(
                StreamObserver<Zenbpm.JobStreamResponse> observer) {
            streamCount.incrementAndGet();
            responseObserver.set(observer);
            return new StreamObserver<Zenbpm.JobStreamRequest>() {
                @Override
                public void onNext(Zenbpm.JobStreamRequest request) {
                    if (request.hasSubscription()
                            && request.getSubscription().getType()
                            == Zenbpm.StreamSubscriptionRequest.Type.TYPE_SUBSCRIBE) {
                        subscriptionCounts
                                .computeIfAbsent(request.getSubscription().getJobType(), ignored -> new AtomicInteger())
                                .incrementAndGet();
                    }
                    if (request.hasComplete() && request.getComplete().getKey() == 42L) {
                        completed.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    // The server restart intentionally cancels the first stream.
                }

                @Override
                public void onCompleted() {
                    observer.onCompleted();
                }
            };
        }

        private int subscriptionCount(String jobType) {
            AtomicInteger count = subscriptionCounts.get(jobType);
            return count == null ? 0 : count.get();
        }

        private void sendJob() {
            Zenbpm.WaitingJob job = Zenbpm.WaitingJob.newBuilder()
                    .setKey(42L)
                    .setType("test-job")
                    .setInputVariables(ByteString.copyFromUtf8("{}"))
                    .build();
            responseObserver.get().onNext(Zenbpm.JobStreamResponse.newBuilder().setJob(job).build());
        }

        private void completeStream() {
            responseObserver.get().onCompleted();
        }
    }

    private static class RecoveringJobService extends ZenBpmGrpc.ZenBpmImplBase {
        private final AtomicBoolean available = new AtomicBoolean();
        private final AtomicInteger streamCount = new AtomicInteger();
        private final CountDownLatch subscribed = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicReference<StreamObserver<Zenbpm.JobStreamResponse>> responseObserver =
                new AtomicReference<>();

        @Override
        public StreamObserver<Zenbpm.JobStreamRequest> jobStream(
                StreamObserver<Zenbpm.JobStreamResponse> observer) {
            streamCount.incrementAndGet();
            if (!available.get()) {
                observer.onError(Status.UNAVAILABLE.withDescription("temporarily unavailable").asRuntimeException());
                return new NoopRequestObserver();
            }
            responseObserver.set(observer);
            return new StreamObserver<Zenbpm.JobStreamRequest>() {
                @Override
                public void onNext(Zenbpm.JobStreamRequest request) {
                    if (request.hasSubscription()) {
                        subscribed.countDown();
                    }
                    if (request.hasComplete()) {
                        completed.countDown();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    // No action needed in the test service.
                }

                @Override
                public void onCompleted() {
                    observer.onCompleted();
                }
            };
        }

        private void sendJob() {
            Zenbpm.WaitingJob job = Zenbpm.WaitingJob.newBuilder()
                    .setKey(42L)
                    .setType("test-job")
                    .setInputVariables(ByteString.copyFromUtf8("{}"))
                    .build();
            responseObserver.get().onNext(Zenbpm.JobStreamResponse.newBuilder().setJob(job).build());
        }
    }

    private static class FlappingJobService extends ZenBpmGrpc.ZenBpmImplBase {
        private final CountDownLatch streamsOpened;
        private final List<Long> openedAtNanos = Collections.synchronizedList(new ArrayList<>());

        private FlappingJobService(int expectedStreams) {
            streamsOpened = new CountDownLatch(expectedStreams);
        }

        @Override
        public StreamObserver<Zenbpm.JobStreamRequest> jobStream(
                StreamObserver<Zenbpm.JobStreamResponse> observer) {
            openedAtNanos.add(System.nanoTime());
            streamsOpened.countDown();
            return new NoopRequestObserver() {
                private boolean terminated;

                @Override
                public void onNext(Zenbpm.JobStreamRequest request) {
                    if (terminated || !request.hasSubscription()) {
                        return;
                    }
                    terminated = true;
                    Zenbpm.ErrorResult error = Zenbpm.ErrorResult.newBuilder()
                            .setCode(1)
                            .setMessage("stream will close")
                            .build();
                    observer.onNext(Zenbpm.JobStreamResponse.newBuilder().setError(error).build());
                    observer.onError(Status.UNAVAILABLE.asRuntimeException());
                }
            };
        }
    }

    private static class NoopRequestObserver implements StreamObserver<Zenbpm.JobStreamRequest> {
        @Override
        public void onNext(Zenbpm.JobStreamRequest request) {
            // No action needed.
        }

        @Override
        public void onError(Throwable throwable) {
            // No action needed.
        }

        @Override
        public void onCompleted() {
            // No action needed.
        }
    }

    private static class RecordingManagedChannel extends ManagedChannel {
        private final ManagedChannel delegate;
        private final AtomicInteger resetBackoffCalls = new AtomicInteger();

        private RecordingManagedChannel(ManagedChannel delegate) {
            this.delegate = delegate;
        }

        @Override
        public ManagedChannel shutdown() {
            delegate.shutdown();
            return this;
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public ManagedChannel shutdownNow() {
            delegate.shutdownNow();
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                CallOptions callOptions
        ) {
            return delegate.newCall(methodDescriptor, callOptions);
        }

        @Override
        public String authority() {
            return delegate.authority();
        }

        @Override
        public void resetConnectBackoff() {
            resetBackoffCalls.incrementAndGet();
            delegate.resetConnectBackoff();
        }
    }
}
