package org.zenbpm;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@AutoConfiguration
@EnableConfigurationProperties(ZenbpmClientProperties.class)
public class ZenbpmClientAutoConfiguration {

    @Bean
    public ZenbpmClientService zenbpmApiClient() {
        return new ZenbpmClientService();
    }
}
