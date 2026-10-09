package org.pbinitiative.zenbpm.grpc;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;
import org.jetbrains.annotations.NotNull;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;
import org.pbinitiative.zenbpm.proto.ZenBpmGrpc;
import org.pbinitiative.zenbpm.proto.Zenbpm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ZenbpmJobWorkerManager implements BeanPostProcessor, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ZenbpmJobWorkerManager.class);
    private static final long INITIAL_RECONNECT_DELAY_MILLIS = 100L;
    private static final long MAXIMUM_RECONNECT_DELAY_MILLIS = 30_000L;
    private static final long BACKOFF_RESET_AFTER_MILLIS = 60_000L;
    private static final int JOB_HANDOVER_QUEUE_CAPACITY = 1;

    private final ZenbpmClientProperties properties;
    private final JobDispatcher jobDispatcher;
    private final ManagedChannelFactory channelFactory;
    private final ReconnectBackoff reconnectBackoff;
    private final Object lifecycleMonitor = new Object();

    private volatile boolean running;
    private ManagedChannel channel;
    private SerializedRequestStream activeRequestStream;
    private ScheduledExecutorService reconnectExecutor;
    private ThreadPoolExecutor jobExecutor;
    private ScheduledFuture<?> reconnectTask;
    private long streamGeneration;

    public ZenbpmJobWorkerManager(
            ZenbpmClientProperties properties,
            ObjectProvider<OpenTelemetry> openTelemetry,
            boolean openTelemetryDisabled
    ) {
        this(
                properties,
                new JobDispatcher(properties, openTelemetry, openTelemetryDisabled),
                new DefaultManagedChannelFactory(),
                new ReconnectBackoff(
                        INITIAL_RECONNECT_DELAY_MILLIS,
                        MAXIMUM_RECONNECT_DELAY_MILLIS,
                        BACKOFF_RESET_AFTER_MILLIS,
                        System::nanoTime));
    }

    ZenbpmJobWorkerManager(
            ZenbpmClientProperties properties,
            JobDispatcher jobDispatcher,
            ManagedChannelFactory channelFactory,
            ReconnectBackoff reconnectBackoff
    ) {
        this.properties = properties;
        this.jobDispatcher = jobDispatcher;
        this.channelFactory = channelFactory;
        this.reconnectBackoff = reconnectBackoff;
    }

    @Override
    public Object postProcessAfterInitialization(@NotNull Object bean, @NotNull String beanName)
            throws BeansException {
        if (properties.isJobWorkerEnabled()) {
            jobDispatcher.registerHandlers(bean);
        }
        return bean;
    }

    @Override
    public void start() {
        synchronized (lifecycleMonitor) {
            if (running || !jobDispatcher.hasHandlers()) {
                return;
            }

            channel = channelFactory.create(properties);
            reconnectExecutor = Executors.newSingleThreadScheduledExecutor(
                    daemonThreadFactory("zenbpm-job-worker-reconnect"));
            jobExecutor = createJobExecutor();
            reconnectBackoff.reset();
            running = true;
        }

        openJobStream();
    }

    @Override
    public void stop() {
        SerializedRequestStream requestStreamToClose;
        ManagedChannel channelToClose;
        ScheduledExecutorService reconnectExecutorToClose;
        ExecutorService jobExecutorToClose;

        synchronized (lifecycleMonitor) {
            if (!running) {
                return;
            }

            running = false;
            streamGeneration++;
            requestStreamToClose = activeRequestStream;
            activeRequestStream = null;
            channelToClose = channel;
            channel = null;
            reconnectExecutorToClose = reconnectExecutor;
            reconnectExecutor = null;
            jobExecutorToClose = jobExecutor;
            jobExecutor = null;
            cancelReconnectTask();
        }

        closeQuietly(requestStreamToClose);
        shutdownNow(reconnectExecutorToClose);
        shutdownNow(jobExecutorToClose);
        shutdownChannel(channelToClose);
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void openJobStream() {
        StreamSession session = beginStreamSession();
        if (session == null) {
            return;
        }

        try {
            session.channel.resetConnectBackoff();
            StreamObserver<Zenbpm.JobStreamResponse> responseObserver = new JobStreamResponseObserver(session);
            StreamObserver<Zenbpm.JobStreamRequest> delegate =
                    ZenBpmGrpc.newStub(session.channel).jobStream(responseObserver);
            SerializedRequestStream requestStream = new SerializedRequestStream(delegate);
            session.attach(requestStream);

            if (!activate(session)) {
                closeQuietly(requestStream);
                return;
            }

            subscribeToAllJobTypes(requestStream);
            log.info("Job stream opened and subscriptions sent for {} job type(s)",
                    jobDispatcher.handlerCount());
        } catch (RuntimeException exception) {
            closeQuietly(session.requestStream());
            if (terminate(session)) {
                log.error("Failed to open job stream", exception);
            }
        }
    }

    private StreamSession beginStreamSession() {
        synchronized (lifecycleMonitor) {
            reconnectTask = null;
            if (!running || channel == null || channel.isShutdown()) {
                return null;
            }
            return new StreamSession(++streamGeneration, channel);
        }
    }

    private boolean activate(StreamSession session) {
        synchronized (lifecycleMonitor) {
            if (!isCurrent(session)) {
                return false;
            }
            activeRequestStream = session.requestStream();
            return true;
        }
    }

    private void subscribeToAllJobTypes(SerializedRequestStream requestStream) {
        for (String jobType : jobDispatcher.jobTypes()) {
            Zenbpm.StreamSubscriptionRequest subscription = Zenbpm.StreamSubscriptionRequest.newBuilder()
                    .setJobType(jobType)
                    .setType(Zenbpm.StreamSubscriptionRequest.Type.TYPE_SUBSCRIBE)
                    .build();
            Zenbpm.JobStreamRequest request = Zenbpm.JobStreamRequest.newBuilder()
                    .setSubscription(subscription)
                    .build();
            if (!requestStream.send(request)) {
                throw new IllegalStateException("Job stream closed while subscriptions were being sent");
            }
        }
    }

    private boolean terminate(StreamSession session) {
        session.markClosed();
        long delayMillis;

        synchronized (lifecycleMonitor) {
            if (!isCurrent(session)) {
                return false;
            }

            discardQueuedJobs(session);
            streamGeneration++;
            activeRequestStream = null;
            delayMillis = reconnectBackoff.nextDelay(session.readyAtNanos());
            try {
                reconnectTask = reconnectExecutor.schedule(
                        this::runReconnectTask,
                        delayMillis,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException exception) {
                if (running) {
                    log.error("Failed to schedule job stream reconnect", exception);
                }
                return true;
            }
        }

        log.info("Reconnecting job stream in {} ms", delayMillis);
        return true;
    }

    private boolean isCurrent(StreamSession session) {
        return running && session.generation == streamGeneration;
    }

    private void runReconnectTask() {
        try {
            openJobStream();
        } catch (Error error) {
            log.error("Unrecoverable error while reconnecting the job stream", error);
            throw error;
        }
    }

    private void submitJob(StreamSession session, Zenbpm.WaitingJob job) {
        ThreadPoolExecutor executor;
        synchronized (lifecycleMonitor) {
            if (!isCurrent(session) || jobExecutor == null) {
                return;
            }
            executor = jobExecutor;
        }

        try {
            executor.execute(new JobTask(session, job));
        } catch (RejectedExecutionException exception) {
            if (terminate(session)) {
                log.error("Job executor rejected job '{}'; reconnecting the stream", job.getKey(), exception);
                session.cancel("Job executor rejected a job", exception);
            }
        }
    }

    private void discardQueuedJobs(StreamSession session) {
        if (jobExecutor != null) {
            jobExecutor.getQueue().removeIf(task ->
                    task instanceof JobTask && ((JobTask) task).belongsTo(session));
        }
    }

    private boolean isCurrentSession(StreamSession session) {
        synchronized (lifecycleMonitor) {
            return isCurrent(session);
        }
    }

    private void requestNextResponse(StreamSession session) {
        if (!isCurrentSession(session)) {
            return;
        }
        session.requestNextResponse();
    }

    private void markStreamReady(
            StreamSession session,
            ClientCallStreamObserver<Zenbpm.JobStreamRequest> stream
    ) {
        if (!stream.isReady()) {
            return;
        }
        synchronized (lifecycleMonitor) {
            if (isCurrent(session)) {
                session.markReady(reconnectBackoff.now());
            }
        }
    }

    private void handleResponse(StreamSession session, Zenbpm.JobStreamResponse response) {
        synchronized (lifecycleMonitor) {
            if (!isCurrent(session)) {
                return;
            }
        }

        if (response.hasError()) {
            log.error("Server error: {}: {}", response.getError().getCode(), response.getError().getMessage());
        } else if (response.hasJob()) {
            submitJob(session, response.getJob());
            return;
        }
        requestNextResponse(session);
    }

    private void handleStreamError(StreamSession session, Throwable throwable) {
        if (!terminate(session)) {
            return;
        }

        Status status = Status.fromThrowable(throwable);
        Status.Code code = status.getCode();
        if (isExpectedTransportFailure(code)) {
            log.warn("Job stream ended with {}: {}", code, status.getDescription());
        } else {
            log.error("Job stream failed with " + code, throwable);
        }
    }

    private void handleStreamCompletion(StreamSession session) {
        if (terminate(session)) {
            log.info("Stream completed by server");
        }
    }

    private void cancelReconnectTask() {
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
            reconnectTask = null;
        }
    }

    private static boolean isExpectedTransportFailure(Status.Code code) {
        return code == Status.Code.UNAVAILABLE
                || code == Status.Code.CANCELLED
                || code == Status.Code.DEADLINE_EXCEEDED;
    }

    private static ThreadPoolExecutor createJobExecutor() {
        return new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(JOB_HANDOVER_QUEUE_CAPACITY),
                daemonThreadFactory("zenbpm-job-worker-handler"),
                new ThreadPoolExecutor.AbortPolicy());
    }

    private static ThreadFactory daemonThreadFactory(String threadName) {
        return runnable -> {
            Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        };
    }

    private static void closeQuietly(SerializedRequestStream requestStream) {
        if (requestStream == null) {
            return;
        }
        try {
            requestStream.close();
        } catch (RuntimeException exception) {
            log.debug("Error closing obsolete request stream: {}", exception.getMessage());
        }
    }

    private static void shutdownNow(ExecutorService executor) {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private static void shutdownChannel(ManagedChannel channel) {
        if (channel == null) {
            return;
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(3L, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }

    private final class JobStreamResponseObserver implements
            ClientResponseObserver<Zenbpm.JobStreamRequest, Zenbpm.JobStreamResponse> {
        private final StreamSession session;

        private JobStreamResponseObserver(StreamSession session) {
            this.session = session;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<Zenbpm.JobStreamRequest> stream) {
            session.attachResponseController(stream);
            stream.disableAutoRequestWithInitial(1);
            stream.setOnReadyHandler(() -> markStreamReady(session, stream));
        }

        @Override
        public void onNext(Zenbpm.JobStreamResponse response) {
            handleResponse(session, response);
        }

        @Override
        public void onError(Throwable throwable) {
            handleStreamError(session, throwable);
        }

        @Override
        public void onCompleted() {
            handleStreamCompletion(session);
        }
    }

    private final class JobTask implements Runnable {
        private final StreamSession session;
        private final Zenbpm.WaitingJob job;

        private JobTask(StreamSession session, Zenbpm.WaitingJob job) {
            this.session = session;
            this.job = job;
        }

        @Override
        public void run() {
            try {
                if (isCurrentSession(session)) {
                    jobDispatcher.dispatch(job, session.requestStream());
                }
            } finally {
                requestNextResponse(session);
            }
        }

        private boolean belongsTo(StreamSession candidate) {
            return session == candidate;
        }
    }

    private static final class StreamSession {
        private final long generation;
        private final ManagedChannel channel;
        private final AtomicReference<SerializedRequestStream> requestStream = new AtomicReference<>();
        private final AtomicReference<ClientCallStreamObserver<Zenbpm.JobStreamRequest>> responseController =
                new AtomicReference<>();
        private final AtomicReference<Long> readyAtNanos = new AtomicReference<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        private StreamSession(long generation, ManagedChannel channel) {
            this.generation = generation;
            this.channel = channel;
        }

        private void attach(SerializedRequestStream stream) {
            requestStream.set(stream);
        }

        private SerializedRequestStream requestStream() {
            return requestStream.get();
        }

        private void attachResponseController(
                ClientCallStreamObserver<Zenbpm.JobStreamRequest> controller
        ) {
            responseController.set(controller);
        }

        private void requestNextResponse() {
            ClientCallStreamObserver<Zenbpm.JobStreamRequest> controller = responseController.get();
            if (!closed.get() && controller != null) {
                controller.request(1);
            }
        }

        private void markReady(long readyAt) {
            if (!closed.get()) {
                readyAtNanos.compareAndSet(null, readyAt);
            }
        }

        private Long readyAtNanos() {
            return readyAtNanos.get();
        }

        private void cancel(String message, Throwable cause) {
            ClientCallStreamObserver<Zenbpm.JobStreamRequest> controller = responseController.get();
            if (controller != null) {
                controller.cancel(message, cause);
            }
        }

        private void markClosed() {
            closed.set(true);
            SerializedRequestStream stream = requestStream();
            if (stream != null) {
                stream.markClosed();
            }
        }
    }
}
