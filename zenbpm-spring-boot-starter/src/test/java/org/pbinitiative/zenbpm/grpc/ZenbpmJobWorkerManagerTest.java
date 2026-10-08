package org.pbinitiative.zenbpm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Server;
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
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        manager = createManager(port, handled);
        manager.start();

        assertTrue(firstService.subscribed.await(5, TimeUnit.SECONDS),
                "worker should subscribe on the initial stream");

        server.shutdownNow();
        assertTrue(server.awaitTermination(5, TimeUnit.SECONDS),
                "initial server should stop");

        RecordingJobService restartedService = new RecordingJobService();
        server = startServer(port, restartedService);

        assertTrue(restartedService.subscribed.await(10, TimeUnit.SECONDS),
                "worker should reconnect and restore its subscription");

        restartedService.sendJob();
        assertTrue(handled.await(5, TimeUnit.SECONDS),
                "reconnected worker should dispatch jobs");
        assertTrue(restartedService.completed.await(5, TimeUnit.SECONDS),
                "reconnected worker should complete jobs on the new stream");
    }

    @Test
    void connectsWhenServerBecomesAvailableAfterStartup() throws Exception {
        int port = findAvailablePort();
        CountDownLatch handled = new CountDownLatch(1);
        manager = createManager(port, handled);
        manager.start();

        Thread.sleep(500L);

        RecordingJobService service = new RecordingJobService();
        server = startServer(port, service);

        assertTrue(service.subscribed.await(10, TimeUnit.SECONDS),
                "worker should keep retrying until the server becomes available");

        service.sendJob();
        assertTrue(handled.await(5, TimeUnit.SECONDS),
                "worker connected after startup should dispatch jobs");
        assertTrue(service.completed.await(5, TimeUnit.SECONDS),
                "worker connected after startup should complete jobs");
    }

    @Test
    void doesNotSendFailureOrThrowWhenCompletionCannotBeSent() throws Exception {
        CountDownLatch handled = new CountDownLatch(1);
        manager = createManager(1, handled);
        AtomicInteger completionAttempts = new AtomicInteger();
        AtomicInteger failureAttempts = new AtomicInteger();
        StreamObserver<Zenbpm.JobStreamRequest> closedStream = new StreamObserver<Zenbpm.JobStreamRequest>() {
            @Override
            public void onNext(Zenbpm.JobStreamRequest request) {
                if (request.hasComplete()) {
                    completionAttempts.incrementAndGet();
                }
                if (request.hasFail()) {
                    failureAttempts.incrementAndGet();
                }
                throw new IllegalStateException("stream is already closed");
            }

            @Override
            public void onError(Throwable throwable) {
                // Not used by the manager when dispatching a job response.
            }

            @Override
            public void onCompleted() {
                // Not used by the manager when dispatching a job response.
            }
        };
        Zenbpm.WaitingJob job = Zenbpm.WaitingJob.newBuilder()
                .setKey(42L)
                .setType("test-job")
                .setInputVariables(ByteString.copyFromUtf8("{}"))
                .build();
        Method dispatchJob = ZenbpmJobWorkerManager.class.getDeclaredMethod(
                "dispatchJob", Zenbpm.WaitingJob.class, StreamObserver.class);
        dispatchJob.setAccessible(true);

        assertDoesNotThrow(() -> dispatchJob.invoke(manager, job, closedStream));

        assertEquals(1, completionAttempts.get(), "completion should be attempted once");
        assertEquals(0, failureAttempts.get(), "a transport failure is not a handler failure");
    }

    private Server startServer(int port, RecordingJobService service) throws IOException {
        return NettyServerBuilder.forPort(port)
                .addService(service)
                .build()
                .start();
    }

    private int findAvailablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private ZenbpmJobWorkerManager createManager(int port, CountDownLatch handled) {
        ZenbpmClientProperties properties = new ZenbpmClientProperties();
        properties.setGrpcHost("127.0.0.1");
        properties.setGrpcPort(port);
        properties.setGrpcPlaintext(true);
        properties.setGrpcLoggingEnabled(false);
        properties.setJobWorkerEnabled(true);

        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        ObjectProvider<OpenTelemetry> openTelemetry = beanFactory.getBeanProvider(OpenTelemetry.class);
        ZenbpmJobWorkerManager workerManager = new ZenbpmJobWorkerManager(properties, openTelemetry, true);
        workerManager.postProcessAfterInitialization(new TestWorker(handled), "testWorker");
        return workerManager;
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

    private static class RecordingJobService extends ZenBpmGrpc.ZenBpmImplBase {
        private final CountDownLatch subscribed = new CountDownLatch(1);
        private final CountDownLatch completed = new CountDownLatch(1);
        private final AtomicReference<StreamObserver<Zenbpm.JobStreamResponse>> responseObserver =
                new AtomicReference<>();

        @Override
        public StreamObserver<Zenbpm.JobStreamRequest> jobStream(
                StreamObserver<Zenbpm.JobStreamResponse> observer) {
            responseObserver.set(observer);
            return new StreamObserver<Zenbpm.JobStreamRequest>() {
                @Override
                public void onNext(Zenbpm.JobStreamRequest request) {
                    if (request.hasSubscription()
                            && request.getSubscription().getType()
                            == Zenbpm.StreamSubscriptionRequest.Type.TYPE_SUBSCRIBE
                            && "test-job".equals(request.getSubscription().getJobType())) {
                        subscribed.countDown();
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

        private void sendJob() {
            Zenbpm.WaitingJob job = Zenbpm.WaitingJob.newBuilder()
                    .setKey(42L)
                    .setType("test-job")
                    .setInputVariables(ByteString.copyFromUtf8("{}"))
                    .build();
            responseObserver.get().onNext(Zenbpm.JobStreamResponse.newBuilder().setJob(job).build());
        }
    }
}
