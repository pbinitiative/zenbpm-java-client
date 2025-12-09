package org.zenbpm;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zenbpm")
public class ZenbpmClientProperties {

    private String baseUrl;

    public String getBaseUrl() {
        if (baseUrl == null) {
            return "http://localhost:8080/v1";
        }
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
}
