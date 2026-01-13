package org.zenbpm.rest;

import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.zenbpm.ZenbpmClientProperties;
import org.zenbpm.client.ApiClient;

@Service
public class ZenbpmClientService {

    private static final Logger log = LoggerFactory.getLogger(ZenbpmClientService.class);

    private final ZenbpmClientProperties properties;

    private ApiClient apiClient;

    public ApiClient getApiClient() {
        initApiClient();
        return apiClient;
    }

    public ZenbpmClientService(ZenbpmClientProperties properties) {
        this.properties = properties;
    }

    private void initApiClient() {
        if (apiClient == null) {
            apiClient = new ApiClient();
            apiClient.setBasePath(properties.getRestUrl());
            if (properties.isRestLoggingEnabled()) {
                HttpLoggingInterceptor.Logger slf4jLogger = this::logHttpMessage;
                HttpLoggingInterceptor interceptor = new HttpLoggingInterceptor(slf4jLogger);
                interceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
                OkHttpClient client = apiClient.getHttpClient()
                        .newBuilder()
                        .addInterceptor(interceptor)
                        .build();
                apiClient.setHttpClient(client);
            }
        }
    }

    private void logHttpMessage(String message) {
        log.debug(message);
    }

}
