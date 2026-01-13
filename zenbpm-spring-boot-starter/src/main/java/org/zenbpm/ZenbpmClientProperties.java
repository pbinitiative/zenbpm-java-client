package org.zenbpm;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("zenbpm")
public class ZenbpmClientProperties {

    // REST base URL (existing functionality)
    private String restUrl = "http://localhost:8080/v1";

    // REST logging
    private boolean restLoggingEnabled = true;

    // gRPC connection properties
    private String grpcHost = "localhost";
    private int grpcPort = 9090;
    private boolean grpcPlaintext = true; // set to false when using TLS

    // gRPC logging
    private boolean grpcLoggingEnabled = true;

    private boolean jobWorkerEnabled = true;

    public String getRestUrl() {
        return restUrl;
    }

    public void setRestUrl(String restUrl) {
        this.restUrl = restUrl;
    }

    public boolean isRestLoggingEnabled() {
        return restLoggingEnabled;
    }

    public void setRestLoggingEnabled(boolean restLoggingEnabled) {
        this.restLoggingEnabled = restLoggingEnabled;
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

    public boolean isGrpcLoggingEnabled() {
        return grpcLoggingEnabled;
    }

    public void setGrpcLoggingEnabled(boolean grpcLoggingEnabled) {
        this.grpcLoggingEnabled = grpcLoggingEnabled;
    }

    public boolean isJobWorkerEnabled() {
        return jobWorkerEnabled;
    }

    public void setJobWorkerEnabled(boolean jobWorkerEnabled) {
        this.jobWorkerEnabled = jobWorkerEnabled;
    }
}
