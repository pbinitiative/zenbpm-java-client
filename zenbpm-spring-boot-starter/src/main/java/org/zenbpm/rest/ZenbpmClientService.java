package org.zenbpm.rest;

import io.opentelemetry.api.OpenTelemetry;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import org.zenbpm.ZenbpmClientProperties;
import org.zenbpm.client.ApiClient;

@Service
public class ZenbpmClientService {

    private static final Logger log = LoggerFactory.getLogger(ZenbpmClientService.class);

    private final ZenbpmClientProperties properties;
    private final ObjectProvider<OpenTelemetry> openTelemetry;

    private ApiClient apiClient;

    public ApiClient getApiClient() {
        initApiClient();
        return apiClient;
    }

    public ZenbpmClientService(ZenbpmClientProperties properties, ObjectProvider<OpenTelemetry> openTelemetry) {
        this.properties = properties;
        this.openTelemetry = openTelemetry;
    }

    private void initApiClient() {
        if (apiClient == null) {
            apiClient = new ApiClient();
            apiClient.setBasePath(properties.getRestUrl());

            OkHttpClient.Builder builder = apiClient.getHttpClient().newBuilder();

            OpenTelemetry otel = openTelemetry.getIfAvailable();
            if (properties.isOtelEnabled() && otel != null) {
                builder.addInterceptor(new ZenbpmOkHttpOtelInterceptor(otel));
            }

            if (properties.isRestLoggingEnabled()) {
                HttpLoggingInterceptor.Logger slf4jLogger = this::logHttpMessage;
                HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor(slf4jLogger);
                interceptor.setLevel(
                        log.isTraceEnabled()
                                ? HttpLoggingInterceptor.Level.BODY
                                : HttpLoggingInterceptor.Level.BASIC
                );
                builder.addInterceptor(interceptor);
            }

            apiClient.setHttpClient(builder.build());
        }
    }

    private void logHttpMessage(String message) {
        if (log.isTraceEnabled()) {
            log.trace(message);
            return;
        }
        log.debug(message);
    }

}
