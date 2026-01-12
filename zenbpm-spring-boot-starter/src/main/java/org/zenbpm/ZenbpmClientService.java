package org.zenbpm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zenbpm.client.ApiClient;

@Service
public class ZenbpmClientService {

    private ApiClient apiClient;

    @Autowired
    private ZenbpmClientProperties properties;

    public ZenbpmClient getEngineStatus() {
        initApiClient();
        return new ZenbpmClient(apiClient);
    }

    public ApiClient getApiClient() {
        initApiClient();
        return apiClient;
    }

    private void initApiClient() {
        if (apiClient == null) {
            apiClient = new ApiClient();
            apiClient.setBasePath(properties.getRestUrl());
        }
    }
}
