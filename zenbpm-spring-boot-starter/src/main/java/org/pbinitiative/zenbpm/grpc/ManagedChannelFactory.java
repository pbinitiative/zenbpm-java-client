package org.pbinitiative.zenbpm.grpc;

import io.grpc.ManagedChannel;
import org.pbinitiative.zenbpm.ZenbpmClientProperties;

@FunctionalInterface
interface ManagedChannelFactory {
    ManagedChannel create(ZenbpmClientProperties properties);
}
