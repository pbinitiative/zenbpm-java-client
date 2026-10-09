package org.pbinitiative.zenbpm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;
import org.pbinitiative.zenbpm.proto.Zenbpm;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JobDispatcherTest {

    @Test
    void doesNotFailAJobWhenItsCompletionCannotBeSent() {
        JobDispatcher dispatcher = createDispatcher(new SuccessfulWorker());
        AtomicInteger completionAttempts = new AtomicInteger();
        AtomicInteger failureAttempts = new AtomicInteger();
        SerializedRequestStream closedStream = new SerializedRequestStream(
                new NoopRequestObserver() {
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
                });

        assertDoesNotThrow(() -> dispatcher.dispatch(job(), closedStream));
        assertEquals(1, completionAttempts.get());
        assertEquals(0, failureAttempts.get());
    }

    @Test
    void sendsTheHandlersOriginalFailureMessage() {
        JobDispatcher dispatcher = createDispatcher(new FailingWorker());
        AtomicReference<Zenbpm.JobFailRequest> failure = new AtomicReference<>();
        SerializedRequestStream stream = new SerializedRequestStream(new NoopRequestObserver() {
            @Override
            public void onNext(Zenbpm.JobStreamRequest request) {
                if (request.hasFail()) {
                    failure.set(request.getFail());
                }
            }
        });

        dispatcher.dispatch(job(), stream);

        assertEquals("handler failed", failure.get().getMessage());
        assertTrue(failure.get().getVariables().toStringUtf8().contains("handler failed"));
    }

    private static JobDispatcher createDispatcher(Object worker) {
        ZenbpmClientProperties properties = new ZenbpmClientProperties();
        properties.setGrpcLoggingEnabled(false);
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        ObjectProvider<OpenTelemetry> openTelemetry = beanFactory.getBeanProvider(OpenTelemetry.class);
        JobDispatcher dispatcher = new JobDispatcher(properties, openTelemetry, true);
        dispatcher.registerHandlers(worker);
        return dispatcher;
    }

    private static Zenbpm.WaitingJob job() {
        return Zenbpm.WaitingJob.newBuilder()
                .setKey(42L)
                .setType("test-job")
                .setInputVariables(ByteString.copyFromUtf8("{}"))
                .build();
    }

    private static final class SuccessfulWorker {
        @JobWorker("test-job")
        public Map<String, Object> handle() {
            return Collections.<String, Object>singletonMap("result", "completed");
        }
    }

    private static final class FailingWorker {
        @JobWorker("test-job")
        public void handle() {
            throw new IllegalStateException("handler failed");
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
}
