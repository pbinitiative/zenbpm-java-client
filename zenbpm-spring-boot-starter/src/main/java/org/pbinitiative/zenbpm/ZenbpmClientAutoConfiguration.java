package org.pbinitiative.zenbpm;

import io.opentelemetry.api.OpenTelemetry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.pbinitiative.zenbpm.grpc.ZenbpmJobWorkerManager;
import org.pbinitiative.zenbpm.rest.ZenbpmClientService;

@AutoConfiguration
@EnableConfigurationProperties(ZenbpmClientProperties.class)
public class ZenbpmClientAutoConfiguration {

    private final boolean isOtelDisabled;

    public ZenbpmClientAutoConfiguration(@Value("${otel.sdk.disabled:false}") boolean isOtelDisabled) {
        this.isOtelDisabled = isOtelDisabled;
    }

    @Bean
    public ZenbpmClientService zenbpmApiClient(ZenbpmClientProperties props, ObjectProvider<OpenTelemetry> openTelemetry) {
        return new ZenbpmClientService(props, openTelemetry, isOtelDisabled);
    }

    @Bean
    public ZenbpmJobWorkerManager zenbpmJobWorkerManager(ZenbpmClientProperties props, ObjectProvider<OpenTelemetry> openTelemetry) {
        return new ZenbpmJobWorkerManager(props, openTelemetry, isOtelDisabled);
    }
}
