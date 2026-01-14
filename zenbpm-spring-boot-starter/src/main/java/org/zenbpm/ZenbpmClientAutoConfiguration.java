package org.zenbpm;

import io.opentelemetry.api.OpenTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.zenbpm.grpc.ZenbpmJobWorkerManager;
import org.zenbpm.rest.ZenbpmClientService;

@AutoConfiguration
@EnableConfigurationProperties(ZenbpmClientProperties.class)
public class ZenbpmClientAutoConfiguration {

    @Bean
    public ZenbpmClientService zenbpmApiClient(ZenbpmClientProperties props, ObjectProvider<OpenTelemetry> openTelemetry) {
        return new ZenbpmClientService(props, openTelemetry);
    }

    @Bean
    ZenbpmJobWorkerManager zenbpmJobWorkerManager(ZenbpmClientProperties props, ObjectProvider<OpenTelemetry> openTelemetry) {
        return new ZenbpmJobWorkerManager(props, openTelemetry);
    }
}
