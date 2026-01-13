package org.zenbpm;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.zenbpm.grpc.ZenbpmJobWorkerManager;
import org.zenbpm.rest.ZenbpmClientService;

@AutoConfiguration
@EnableConfigurationProperties(ZenbpmClientProperties.class)
public class ZenbpmClientAutoConfiguration {

    @Bean
    public ZenbpmClientService zenbpmApiClient(ZenbpmClientProperties props) {
        return new ZenbpmClientService(props);
    }

    @Bean
    ZenbpmJobWorkerManager zenbpmJobWorkerManager(ZenbpmClientProperties props) {
        return new ZenbpmJobWorkerManager(props);
    }
}
