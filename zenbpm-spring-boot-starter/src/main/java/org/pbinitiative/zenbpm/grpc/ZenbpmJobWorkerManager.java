package org.pbinitiative.zenbpm.grpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.ReflectionUtils;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;
import org.pbinitiative.zenbpm.proto.ZenBpmGrpc;
import org.pbinitiative.zenbpm.proto.Zenbpm;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ZenbpmJobWorkerManager implements BeanPostProcessor, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ZenbpmJobWorkerManager.class);
    private static final TypeReference<HashMap<String,Object>> MAP_TYPE_REF = new TypeReference<HashMap<String,Object>>() {};
    private static final long RECONNECT_INITIAL_DELAY_MILLIS = 100L;
    private static final long RECONNECT_MAX_DELAY_MILLIS = 30_000L;
    private static final long RECONNECT_BACKOFF_RESET_MILLIS = 60_000L;
    private final ZenbpmClientProperties properties;
    private final ObjectProvider<OpenTelemetry> openTelemetry;
    private final boolean isOtelDisabled;

    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Object lifecycleMonitor = new Object();

    private ManagedChannel channel;
    private StreamObserver<Zenbpm.JobStreamRequest> requestObserver;
    private ScheduledExecutorService reconnectExecutor;
    private ScheduledFuture<?> reconnectTask;
    private ScheduledFuture<?> backoffResetTask;
    private long reconnectDelayMillis = RECONNECT_INITIAL_DELAY_MILLIS;
    private long streamGeneration;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ZenbpmJobWorkerManager(
            ZenbpmClientProperties properties,
            ObjectProvider<OpenTelemetry> openTelemetry,
            boolean isOtelDisabled
    ) {
        this.properties = properties;
        this.openTelemetry = openTelemetry;
        this.isOtelDisabled = isOtelDisabled;
    }

    @Override
    public Object postProcessAfterInitialization(@NotNull Object bean, @NotNull String beanName) throws BeansException {
        if (!properties.isJobWorkerEnabled()) return bean;
        Class<?> targetClass = bean.getClass();
        ReflectionUtils.doWithMethods(targetClass, method -> {
            JobWorker annotation = method.getAnnotation(JobWorker.class);
            if (annotation != null) {
                String jobType = annotation.value();
                method.setAccessible(true);
                handlers.put(jobType, new Handler(bean, method, jobType));
            }
        }, method -> method.isAnnotationPresent(JobWorker.class));
        return bean;
    }

    @Override
    public void start() {
        synchronized (lifecycleMonitor) {
            if (running.get() || handlers.isEmpty()) {
                return;
            }
            ManagedChannelBuilder<?> chBuilder = ManagedChannelBuilder
                    .forAddress(properties.getGrpcHost(), properties.getGrpcPort());
            if (properties.isGrpcPlaintext()) {
                chBuilder = chBuilder.usePlaintext();
            }

            channel = chBuilder.build();
            reconnectExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "zenbpm-job-worker-reconnect");
                thread.setDaemon(true);
                return thread;
            });
            reconnectDelayMillis = RECONNECT_INITIAL_DELAY_MILLIS;
            running.set(true);
        }

        connectJobStream();
    }

    private void connectJobStream() {
        final ManagedChannel activeChannel;
        final long generation;
        synchronized (lifecycleMonitor) {
            reconnectTask = null;
            if (!running.get() || channel == null || channel.isShutdown()) {
                return;
            }
            activeChannel = channel;
            generation = ++streamGeneration;
        }

        AtomicReference<StreamObserver<Zenbpm.JobStreamRequest>> streamReference = new AtomicReference<>();
        StreamObserver<Zenbpm.JobStreamResponse> responseObserver = new StreamObserver<Zenbpm.JobStreamResponse>() {
            @Override
            public void onNext(Zenbpm.JobStreamResponse resp) {
                if (!markStreamHealthyIfCurrent(generation)) {
                    return;
                }
                if (resp.hasError()) {
                    log.error("Server error: {}: {}", resp.getError().getCode(), resp.getError().getMessage());
                    return;
                }
                if (resp.hasJob()) {
                    Zenbpm.WaitingJob job = resp.getJob();
                    dispatchJob(job, streamReference.get());
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Stream error", t);
                handleStreamTermination(generation);
            }

            @Override
            public void onCompleted() {
                log.info("Stream completed by server");
                handleStreamTermination(generation);
            }
        };

        StreamObserver<Zenbpm.JobStreamRequest> newRequestObserver = null;
        try {
            ZenBpmGrpc.ZenBpmStub stub = ZenBpmGrpc.newStub(activeChannel);
            newRequestObserver = stub.jobStream(responseObserver);
            streamReference.set(newRequestObserver);

            synchronized (lifecycleMonitor) {
                if (!running.get() || generation != streamGeneration) {
                    closeRequestObserver(newRequestObserver);
                    return;
                }
                requestObserver = newRequestObserver;
            }

            for (String jobType : getJobTypes()) {
                Zenbpm.StreamSubscriptionRequest subscribe = Zenbpm.StreamSubscriptionRequest.newBuilder()
                        .setJobType(jobType)
                        .setType(Zenbpm.StreamSubscriptionRequest.Type.TYPE_SUBSCRIBE)
                        .build();
                Zenbpm.JobStreamRequest req = Zenbpm.JobStreamRequest.newBuilder().setSubscription(subscribe).build();
                newRequestObserver.onNext(req);
            }

            scheduleBackoffReset(generation);
            log.info("Job stream opened and subscriptions sent for {} job type(s)", handlers.size());
        } catch (RuntimeException ex) {
            if (newRequestObserver != null) {
                closeRequestObserver(newRequestObserver);
            }
            log.error("Failed to open job stream", ex);
            handleStreamTermination(generation);
        }
    }

    private boolean markStreamHealthyIfCurrent(long generation) {
        synchronized (lifecycleMonitor) {
            if (!running.get() || generation != streamGeneration) {
                return false;
            }
            reconnectDelayMillis = RECONNECT_INITIAL_DELAY_MILLIS;
            cancelBackoffResetTask();
            return true;
        }
    }

    private void scheduleBackoffReset(long generation) {
        synchronized (lifecycleMonitor) {
            if (!running.get() || generation != streamGeneration || reconnectExecutor == null) {
                return;
            }
            cancelBackoffResetTask();
            backoffResetTask = reconnectExecutor.schedule(() -> markStreamHealthyIfCurrent(generation),
                    RECONNECT_BACKOFF_RESET_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private void handleStreamTermination(long generation) {
        long delayMillis;
        synchronized (lifecycleMonitor) {
            if (!running.get() || generation != streamGeneration) {
                return;
            }
            streamGeneration++;
            requestObserver = null;
            cancelBackoffResetTask();

            delayMillis = reconnectDelayMillis;
            reconnectDelayMillis = Math.min(RECONNECT_MAX_DELAY_MILLIS, reconnectDelayMillis * 2);
            if (reconnectTask != null && !reconnectTask.isDone()) {
                return;
            }
            try {
                reconnectTask = reconnectExecutor.schedule(this::connectJobStream, delayMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ex) {
                if (running.get()) {
                    log.error("Failed to schedule job stream reconnect", ex);
                }
                return;
            }
        }
        log.info("Reconnecting job stream in {} ms", delayMillis);
    }

    private void cancelBackoffResetTask() {
        if (backoffResetTask != null) {
            backoffResetTask.cancel(false);
            backoffResetTask = null;
        }
    }

    private void closeRequestObserver(StreamObserver<Zenbpm.JobStreamRequest> observer) {
        try {
            observer.onCompleted();
        } catch (RuntimeException ex) {
            log.debug("Error closing obsolete request observer: {}", ex.getMessage());
        }
    }

    private Collection<String> getJobTypes() {
        Set<String> keys = handlers.keySet();
        return keys.isEmpty() ? Collections.emptyList() : keys;
    }

    private void dispatchJob(Zenbpm.WaitingJob job, StreamObserver<Zenbpm.JobStreamRequest> stream) {
        Handler handler = handlers.get(job.getType());
        if (handler == null || stream == null) {
            // Ignore unknown job types
            return;
        }

        OpenTelemetry otel = !isOtelDisabled ? openTelemetry.getIfAvailable() : null;
        Tracer tracer = (otel != null) ? otel.getTracer("org.pbinitiative.zenbpm.grpc") : null;

        Span span = (tracer != null)
                ? tracer.spanBuilder("zenbpm.job.process").setSpanKind(SpanKind.CONSUMER).startSpan()
                : null;

        if (span != null) {
            span.setAttribute("zenbpm.job.type", job.getType());
            span.setAttribute("zenbpm.job.key", job.getKey());
        }

        try (Scope scope = (span != null) ? span.makeCurrent() : null) {
            MDC.put("job_key", Long.toString(job.getKey()));
            if (properties.isGrpcLoggingEnabled()) {
                log.debug("Starting job processing for type '{}', key '{}'", job.getType(), job.getKey());
                log.trace("Job variables: {}", job.getInputVariables());
            }
            Object result;
            Class<?>[] paramTypes = handler.method.getParameterTypes();
            if (paramTypes.length == 0) {
                result = handler.method.invoke(handler.bean);
            } else if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(Zenbpm.WaitingJob.class)) {
                result = handler.method.invoke(handler.bean, job);
            } else if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(JobContext.class)) {
                Map<String, Object> variables = objectMapper.readValue(job.getInputVariables().newInput(), MAP_TYPE_REF);
                JobContext context = new JobContext(job, variables);
                result = handler.method.invoke(handler.bean, context);
            } else if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(Map.class)) {
                Map<String, Object> variables = objectMapper.readValue(job.getInputVariables().newInput(), MAP_TYPE_REF);
                result = handler.method.invoke(handler.bean, variables);
            } else {
                throw new IllegalArgumentException("@JobWorker method must have 0 params or a single WaitingJob/JobContext/Map<String, Object> param");
            }

            ByteString vars = serializeResult(result);
            Zenbpm.JobCompleteRequest complete = Zenbpm.JobCompleteRequest.newBuilder()
                    .setKey(job.getKey())
                    .setVariables(vars)
                    .build();
            Zenbpm.JobStreamRequest completeReq = Zenbpm.JobStreamRequest.newBuilder().setComplete(complete).build();
            boolean completionSent = sendJobRequest(stream, completeReq, "completion", job.getKey());
            if (completionSent && properties.isGrpcLoggingEnabled()) {
                log.debug("Successfully completed job '{}'", job.getKey());
                log.trace("Job result: {}", result);
            }
        } catch (Exception ex) {
            if (span != null) {
                span.recordException(ex);
                span.setStatus(StatusCode.ERROR);
            }

            String msg = ex.getMessage() == null ? ex.toString() : ex.getMessage();
            ByteString vars = ByteString.EMPTY;
            try {
                vars = serializeResult(Collections.singletonMap("error", msg));
            } catch (Exception e) {
                log.warn("Failed to serialize error variables: {}", e.getMessage());
            }
            Zenbpm.JobFailRequest fail = Zenbpm.JobFailRequest.newBuilder()
                    .setKey(job.getKey())
                    .setMessage(msg)
                    .setErrorCode("HANDLER_ERROR")
                    .setVariables(vars)
                    .build();
            Zenbpm.JobStreamRequest failReq = Zenbpm.JobStreamRequest.newBuilder().setFail(fail).build();
            sendJobRequest(stream, failReq, "failure", job.getKey());
            log.error("Failed to process job '{}': {}", job.getKey(), msg, ex);
        } finally {
            MDC.remove("job_key");
            if (span != null) {
                span.end();
            }
        }
    }

    private boolean sendJobRequest(
            StreamObserver<Zenbpm.JobStreamRequest> stream,
            Zenbpm.JobStreamRequest request,
            String responseType,
            long jobKey
    ) {
        try {
            stream.onNext(request);
            return true;
        } catch (RuntimeException ex) {
            log.warn("Failed to send job {} for '{}'; the job may be redelivered", responseType, jobKey, ex);
            return false;
        }
    }

    private ByteString serializeResult(Object result) throws JsonProcessingException {
        if (result == null) {
            return ByteString.copyFromUtf8("null");
        }
        if (result instanceof byte[]) {
            return ByteString.copyFrom((byte[]) result);
        }
        if (result instanceof String) {
            return ByteString.copyFromUtf8((String) result);
        }
        String json = objectMapper.writeValueAsString(result);
        return ByteString.copyFromUtf8(json);
    }

    @Override
    public void stop() {
        StreamObserver<Zenbpm.JobStreamRequest> observerToClose;
        ManagedChannel channelToClose;
        ScheduledExecutorService executorToClose;
        synchronized (lifecycleMonitor) {
            if (!running.getAndSet(false)) {
                return;
            }
            streamGeneration++;
            observerToClose = requestObserver;
            requestObserver = null;
            channelToClose = channel;
            channel = null;
            executorToClose = reconnectExecutor;
            reconnectExecutor = null;
            if (reconnectTask != null) {
                reconnectTask.cancel(false);
                reconnectTask = null;
            }
            cancelBackoffResetTask();
        }

        try {
            if (observerToClose != null) {
                closeRequestObserver(observerToClose);
            }
            if (executorToClose != null) {
                executorToClose.shutdownNow();
            }
            if (channelToClose != null) {
                channelToClose.shutdown();
                if (!channelToClose.awaitTermination(3, TimeUnit.SECONDS)) {
                    channelToClose.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private static class Handler {
        final Object bean;
        final Method method;
        final String jobType;
        Handler(Object bean, Method method, String jobType) {
            this.bean = bean;
            this.method = method;
            this.jobType = jobType;
        }
    }
}
