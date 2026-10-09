package org.pbinitiative.zenbpm.grpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;
import org.pbinitiative.zenbpm.proto.Zenbpm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.ReflectionUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class JobDispatcher {

    private static final Logger log = LoggerFactory.getLogger(JobDispatcher.class);
    private static final TypeReference<Map<String, Object>> VARIABLES_TYPE =
            new TypeReference<Map<String, Object>>() { };

    private final ZenbpmClientProperties properties;
    private final ObjectProvider<OpenTelemetry> openTelemetry;
    private final boolean openTelemetryDisabled;
    private final ObjectMapper objectMapper;
    private final Map<String, RegisteredHandler> handlers = new ConcurrentHashMap<>();

    JobDispatcher(
            ZenbpmClientProperties properties,
            ObjectProvider<OpenTelemetry> openTelemetry,
            boolean openTelemetryDisabled
    ) {
        this(properties, openTelemetry, openTelemetryDisabled, new ObjectMapper());
    }

    JobDispatcher(
            ZenbpmClientProperties properties,
            ObjectProvider<OpenTelemetry> openTelemetry,
            boolean openTelemetryDisabled,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.openTelemetry = openTelemetry;
        this.openTelemetryDisabled = openTelemetryDisabled;
        this.objectMapper = objectMapper;
    }

    void registerHandlers(Object bean) {
        ReflectionUtils.doWithMethods(
                bean.getClass(),
                method -> registerHandler(bean, method),
                method -> method.isAnnotationPresent(JobWorker.class));
    }

    boolean hasHandlers() {
        return !handlers.isEmpty();
    }

    int handlerCount() {
        return handlers.size();
    }

    Set<String> jobTypes() {
        return handlers.keySet();
    }

    void dispatch(Zenbpm.WaitingJob job, SerializedRequestStream stream) {
        RegisteredHandler handler = handlers.get(job.getType());
        if (handler == null || stream == null) {
            return;
        }

        Span span = startSpan(job);
        try (Scope ignored = span == null ? null : span.makeCurrent()) {
            MDC.put("job_key", Long.toString(job.getKey()));
            logJobStart(job);

            Object result = handler.invoke(job, objectMapper);
            sendCompletion(stream, job, result);
        } catch (Exception exception) {
            Throwable failure = unwrapInvocationFailure(exception);
            recordFailure(span, failure);
            sendFailure(stream, job, failure);
        } finally {
            MDC.remove("job_key");
            if (span != null) {
                span.end();
            }
        }
    }

    private void registerHandler(Object bean, Method method) {
        JobWorker annotation = method.getAnnotation(JobWorker.class);
        method.setAccessible(true);
        handlers.put(annotation.value(), new RegisteredHandler(bean, method));
    }

    private Span startSpan(Zenbpm.WaitingJob job) {
        if (openTelemetryDisabled) {
            return null;
        }
        OpenTelemetry telemetry = openTelemetry.getIfAvailable();
        if (telemetry == null) {
            return null;
        }

        Tracer tracer = telemetry.getTracer("org.pbinitiative.zenbpm.grpc");
        Span span = tracer.spanBuilder("zenbpm.job.process")
                .setSpanKind(SpanKind.CONSUMER)
                .startSpan();
        span.setAttribute("zenbpm.job.type", job.getType());
        span.setAttribute("zenbpm.job.key", job.getKey());
        return span;
    }

    private void logJobStart(Zenbpm.WaitingJob job) {
        if (!properties.isGrpcLoggingEnabled()) {
            return;
        }
        log.debug("Starting job processing for type '{}', key '{}'", job.getType(), job.getKey());
        log.trace("Job variables: {}", job.getInputVariables());
    }

    private void sendCompletion(
            SerializedRequestStream stream,
            Zenbpm.WaitingJob job,
            Object result
    ) throws JsonProcessingException {
        Zenbpm.JobCompleteRequest completion = Zenbpm.JobCompleteRequest.newBuilder()
                .setKey(job.getKey())
                .setVariables(serialize(result))
                .build();
        Zenbpm.JobStreamRequest request = Zenbpm.JobStreamRequest.newBuilder()
                .setComplete(completion)
                .build();

        if (send(stream, request, "completion", job.getKey()) && properties.isGrpcLoggingEnabled()) {
            log.debug("Successfully completed job '{}'", job.getKey());
            log.trace("Job result: {}", result);
        }
    }

    private void sendFailure(
            SerializedRequestStream stream,
            Zenbpm.WaitingJob job,
            Throwable failure
    ) {
        String message = failureMessage(failure);
        Zenbpm.JobFailRequest jobFailure = Zenbpm.JobFailRequest.newBuilder()
                .setKey(job.getKey())
                .setMessage(message)
                .setErrorCode("HANDLER_ERROR")
                .setVariables(serializeFailure(message))
                .build();
        Zenbpm.JobStreamRequest request = Zenbpm.JobStreamRequest.newBuilder()
                .setFail(jobFailure)
                .build();

        send(stream, request, "failure", job.getKey());
        log.error("Failed to process job '{}': {}", job.getKey(), message, failure);
    }

    private boolean send(
            SerializedRequestStream stream,
            Zenbpm.JobStreamRequest request,
            String responseType,
            long jobKey
    ) {
        try {
            if (stream.send(request)) {
                return true;
            }
            log.warn("Failed to send job {} for '{}'; the stream is closed and the job may be redelivered",
                    responseType, jobKey);
        } catch (RuntimeException exception) {
            log.warn("Failed to send job {} for '{}'; the job may be redelivered",
                    responseType, jobKey, exception);
        }
        return false;
    }

    private ByteString serialize(Object value) throws JsonProcessingException {
        if (value == null) {
            return ByteString.copyFromUtf8("null");
        }
        if (value instanceof byte[]) {
            return ByteString.copyFrom((byte[]) value);
        }
        if (value instanceof String) {
            return ByteString.copyFromUtf8((String) value);
        }
        return ByteString.copyFromUtf8(objectMapper.writeValueAsString(value));
    }

    private ByteString serializeFailure(String message) {
        try {
            return serialize(Collections.singletonMap("error", message));
        } catch (JsonProcessingException exception) {
            log.warn("Failed to serialize error variables: {}", exception.getMessage());
            return ByteString.EMPTY;
        }
    }

    private static Throwable unwrapInvocationFailure(Exception exception) {
        if (exception instanceof InvocationTargetException && exception.getCause() != null) {
            return exception.getCause();
        }
        return exception;
    }

    private static String failureMessage(Throwable failure) {
        return failure.getMessage() == null ? failure.toString() : failure.getMessage();
    }

    private static void recordFailure(Span span, Throwable failure) {
        if (span == null) {
            return;
        }
        span.recordException(failure);
        span.setStatus(StatusCode.ERROR);
    }

    private static final class RegisteredHandler {
        private final Object bean;
        private final Method method;

        private RegisteredHandler(Object bean, Method method) {
            this.bean = bean;
            this.method = method;
        }

        private Object invoke(Zenbpm.WaitingJob job, ObjectMapper objectMapper) throws Exception {
            Class<?>[] parameterTypes = method.getParameterTypes();
            if (parameterTypes.length == 0) {
                return method.invoke(bean);
            }
            if (parameterTypes.length != 1) {
                throw invalidSignature();
            }

            Class<?> parameterType = parameterTypes[0];
            if (parameterType.isAssignableFrom(Zenbpm.WaitingJob.class)) {
                return method.invoke(bean, job);
            }

            if (parameterType.isAssignableFrom(JobContext.class)) {
                return method.invoke(bean, new JobContext(job, readVariables(job, objectMapper)));
            }
            if (parameterType.isAssignableFrom(Map.class)) {
                return method.invoke(bean, readVariables(job, objectMapper));
            }
            throw invalidSignature();
        }

        private static Map<String, Object> readVariables(
                Zenbpm.WaitingJob job,
                ObjectMapper objectMapper
        ) throws IOException {
            return objectMapper.readValue(job.getInputVariables().newInput(), VARIABLES_TYPE);
        }

        private static IllegalArgumentException invalidSignature() {
            return new IllegalArgumentException(
                    "@JobWorker method must have no parameters or one WaitingJob, JobContext, or Map parameter");
        }
    }
}
