package org.pbinitiative.zenbpm;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ZenbpmClientPropertiesTest {

    @Test
    void bindsUnitlessKeepAliveDurationsAsSeconds() {
        Map<String, String> values = new HashMap<>();
        values.put("zenbpm.grpc-keep-alive-time", "5");
        values.put("zenbpm.grpc-keep-alive-timeout", "20");
        Binder binder = new Binder(new MapConfigurationPropertySource(values));

        ZenbpmClientProperties properties = binder
                .bind("zenbpm", Bindable.of(ZenbpmClientProperties.class))
                .orElseThrow(IllegalStateException::new);

        assertEquals(Duration.ofSeconds(5), properties.getGrpcKeepAliveTime());
        assertEquals(Duration.ofSeconds(20), properties.getGrpcKeepAliveTimeout());
    }
}
