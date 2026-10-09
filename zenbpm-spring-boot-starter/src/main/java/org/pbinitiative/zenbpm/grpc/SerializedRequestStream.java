package org.pbinitiative.zenbpm.grpc;

import io.grpc.stub.StreamObserver;
import org.pbinitiative.zenbpm.proto.Zenbpm;

/** Serializes all writes to one gRPC request stream and tracks its terminal state. */
final class SerializedRequestStream {

    private final StreamObserver<Zenbpm.JobStreamRequest> delegate;
    private boolean closed;

    SerializedRequestStream(StreamObserver<Zenbpm.JobStreamRequest> delegate) {
        this.delegate = delegate;
    }

    synchronized boolean send(Zenbpm.JobStreamRequest request) {
        if (closed) {
            return false;
        }
        try {
            delegate.onNext(request);
            return true;
        } catch (RuntimeException exception) {
            closed = true;
            throw exception;
        }
    }

    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        delegate.onCompleted();
    }

    synchronized void markClosed() {
        closed = true;
    }
}
