package org.pbinitiative.zenbpm.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

final class DefaultManagedChannelFactory implements ManagedChannelFactory {

    @Override
    public ManagedChannel create(ZenbpmClientProperties properties) {
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder
                .forAddress(properties.getGrpcHost(), properties.getGrpcPort())
                .keepAliveTime(
                        positiveMillis(properties.getGrpcKeepAliveTime(), "zenbpm.grpc-keep-alive-time"),
                        TimeUnit.MILLISECONDS)
                .keepAliveTimeout(
                        positiveMillis(properties.getGrpcKeepAliveTimeout(), "zenbpm.grpc-keep-alive-timeout"),
                        TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(false);
        if (properties.isGrpcPlaintext()) {
            builder = builder.usePlaintext();
        }
        return builder.build();
    }

    private static long positiveMillis(Duration duration, String propertyName) {
        if (duration == null || duration.isZero() || duration.isNegative() || duration.toMillis() == 0L) {
            throw new IllegalArgumentException(propertyName + " must be at least 1 ms");
        }
        return duration.toMillis();
    }
}
