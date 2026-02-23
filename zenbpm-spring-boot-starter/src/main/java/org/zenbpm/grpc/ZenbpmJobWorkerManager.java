package org.zenbpm.grpc;

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
import org.slf4j.MDC;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.ReflectionUtils;
import org.zenbpm.ZenbpmClientProperties;
import org.zenbpm.proto.ZenBpmGrpc;
import org.zenbpm.proto.Zenbpm;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZenbpmJobWorkerManager implements BeanPostProcessor, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ZenbpmJobWorkerManager.class);
    private static final TypeReference<HashMap<String,Object>> MAP_TYPE_REF = new TypeReference<HashMap<String,Object>>() {};
    private final ZenbpmClientProperties properties;
    private final ObjectProvider<OpenTelemetry> openTelemetry;

    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ManagedChannel channel;
    private StreamObserver<Zenbpm.JobStreamRequest> requestObserver;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public ZenbpmJobWorkerManager(ZenbpmClientProperties properties, ObjectProvider<OpenTelemetry> openTelemetry) {
        this.properties = properties;
        this.openTelemetry = openTelemetry;
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
        if (running.get() || handlers.isEmpty()) {
            return;
        }
        ManagedChannelBuilder<?> chBuilder = ManagedChannelBuilder
                .forAddress(properties.getGrpcHost(), properties.getGrpcPort());
        if (properties.isGrpcPlaintext()) {
            chBuilder = chBuilder.usePlaintext();
        }

        channel = chBuilder.build();

        ZenBpmGrpc.ZenBpmStub stub = ZenBpmGrpc.newStub(channel);

        StreamObserver<Zenbpm.JobStreamResponse> responseObserver = new StreamObserver<Zenbpm.JobStreamResponse>() {
            @Override
            public void onNext(Zenbpm.JobStreamResponse resp) {
                if (resp.hasError()) {
                    log.error("Server error: {}: {}", resp.getError().getCode(), resp.getError().getMessage());
                    return;
                }
                if (resp.hasJob()) {
                    Zenbpm.WaitingJob job = resp.getJob();
                    dispatchJob(job);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Stream error", t);
                running.set(false);
            }

            @Override
            public void onCompleted() {
                log.info("Stream completed by server");
                running.set(false);
            }
        };

        requestObserver = stub.jobStream(responseObserver);

        for (String jobType : getJobTypes()) {
            Zenbpm.StreamSubscriptionRequest subscribe = Zenbpm.StreamSubscriptionRequest.newBuilder()
                    .setJobType(jobType)
                    .setType(Zenbpm.StreamSubscriptionRequest.Type.TYPE_SUBSCRIBE)
                    .build();
            Zenbpm.JobStreamRequest req = Zenbpm.JobStreamRequest.newBuilder().setSubscription(subscribe).build();
            requestObserver.onNext(req);
        }

        running.set(true);
    }

    private Collection<String> getJobTypes() {
        Set<String> keys = handlers.keySet();
        return keys.isEmpty() ? Collections.emptyList() : keys;
    }

    private void dispatchJob(Zenbpm.WaitingJob job) {
        Handler handler = handlers.get(job.getType());
        if (handler == null) {
            // Ignore unknown job types
            return;
        }

        OpenTelemetry otel = properties.isOtelEnabled() ? openTelemetry.getIfAvailable() : null;
        Tracer tracer = (otel != null) ? otel.getTracer("org.zenbpm.grpc") : null;

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
                log.trace("Job variables: {}", job.getVariables());
            }
            Object result;
            Class<?>[] paramTypes = handler.method.getParameterTypes();
            if (paramTypes.length == 0) {
                result = handler.method.invoke(handler.bean);
            } else if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(Zenbpm.WaitingJob.class)) {
                result = handler.method.invoke(handler.bean, job);
            } else if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(JobContext.class)) {
                Map<String, Object> variables = objectMapper.readValue(job.getVariables().newInput(), MAP_TYPE_REF);
                JobContext context = new JobContext(job, variables);
                result = handler.method.invoke(handler.bean, context);
            } else if (paramTypes.length == 1 && paramTypes[0].isAssignableFrom(Map.class)) {
                TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
                Map<String, Object> variables = objectMapper.readValue(job.getVariables().newInput(), typeRef);
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
            requestObserver.onNext(completeReq);
            if (properties.isGrpcLoggingEnabled()) {
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
            requestObserver.onNext(failReq);
            log.error("Failed to process job '{}': {}", job.getKey(), msg, ex);
        } finally {
            MDC.remove("job_key");
            if (span != null) {
                span.end();
            }
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
        if (!running.get()) return;
        try {
            if (requestObserver != null) {
                try {
                    requestObserver.onCompleted();
                } catch (Exception e) {
                    log.warn("Error completing request observer: {}", e.getMessage());
                }
            }
            if (channel != null) {
                channel.shutdown();
                channel.awaitTermination(3, TimeUnit.SECONDS);
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
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
