package org.zenbpm;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zenbpm")
public class ZenbpmClientProperties {

    // REST base URL (existing functionality)
    private String restUrl = "http://localhost:8080/v1";

    // gRPC connection properties
    private String grpcHost = "localhost";
    private int grpcPort = 9090;
    private boolean grpcPlaintext = true; // set to false when using TLS

    public String getRestUrl() {
        return restUrl;
    }

    public void setRestUrl(String restUrl) {
        this.restUrl = restUrl;
    }

    public String getGrpcHost() {
        return grpcHost;
    }

    public void setGrpcHost(String grpcHost) {
        this.grpcHost = grpcHost;
    }

    public int getGrpcPort() {
        return grpcPort;
    }

    public void setGrpcPort(int grpcPort) {
        this.grpcPort = grpcPort;
    }

    public boolean isGrpcPlaintext() {
        return grpcPlaintext;
    }

    public void setGrpcPlaintext(boolean grpcPlaintext) {
        this.grpcPlaintext = grpcPlaintext;
    }
}
