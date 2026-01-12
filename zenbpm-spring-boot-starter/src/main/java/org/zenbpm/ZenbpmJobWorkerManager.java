package org.zenbpm;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.ReflectionUtils;
import org.zenbpm.proto.ZenBpmGrpc;
import org.zenbpm.proto.Zenbpm;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

class ZenbpmJobWorkerManager implements BeanPostProcessor, SmartLifecycle {

    private static final TypeReference<HashMap<String,Object>> MAP_TYPE_REF = new TypeReference<HashMap<String,Object>>() {};
    private final ZenbpmClientProperties properties;

    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();

    private volatile boolean running = false;

    private ManagedChannel channel;
    private ZenBpmGrpc.ZenBpmStub stub;
    private StreamObserver<Zenbpm.JobStreamRequest> requestObserver;

    private final ObjectMapper objectMapper = new ObjectMapper();

    ZenbpmJobWorkerManager(ZenbpmClientProperties properties) {
        this.properties = properties;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, @NotNull String beanName) throws BeansException {
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
        if (running) return;
        if (handlers.isEmpty()) {
            return;
        }
        ManagedChannelBuilder<?> chBuilder = ManagedChannelBuilder
                .forAddress(properties.getGrpcHost(), properties.getGrpcPort());
        if (properties.isGrpcPlaintext()) {
            chBuilder = chBuilder.usePlaintext();
        }
        channel = chBuilder.build();

        stub = ZenBpmGrpc.newStub(channel);

        StreamObserver<Zenbpm.JobStreamResponse> responseObserver = new StreamObserver<Zenbpm.JobStreamResponse>() {
            @Override
            public void onNext(Zenbpm.JobStreamResponse resp) {
                if (resp.hasError()) {
                    // TODO: replace logs with slf4j
                    System.err.println("[ZenBPM] Server error: " + resp.getError().getCode() + ": " + resp.getError().getMessage());
                    return;
                }
                if (resp.hasJob()) {
                    Zenbpm.WaitingJob job = resp.getJob();
                    dispatchJob(job);
                }
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("[ZenBPM] Stream error: " + t);
                running = false;
            }

            @Override
            public void onCompleted() {
                System.out.println("[ZenBPM] Stream completed by server");
                running = false;
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

        running = true;
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
        try {
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
        } catch (Exception ex) {
            String msg = ex.getMessage() == null ? ex.toString() : ex.getMessage();
            ByteString vars = ByteString.EMPTY;
            try {
                vars = serializeResult(Collections.singletonMap("error", msg));
            } catch (Exception e) {
                System.err.println("[ZenBPM] Failed to serialize error variables: " + e.getMessage());
            }
            Zenbpm.JobFailRequest fail = Zenbpm.JobFailRequest.newBuilder()
                    .setKey(job.getKey())
                    .setMessage(msg)
                    .setErrorCode("HANDLER_ERROR")
                    .setVariables(vars)
                    .build();
            Zenbpm.JobStreamRequest failReq = Zenbpm.JobStreamRequest.newBuilder().setFail(fail).build();
            requestObserver.onNext(failReq);
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
        if (!running) return;
        try {
            if (requestObserver != null) {
                try {
                    requestObserver.onCompleted();
                } catch (Exception e) {
                    System.err.println("[ZenBPM] Error completing request observer: " + e.getMessage());
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
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
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
