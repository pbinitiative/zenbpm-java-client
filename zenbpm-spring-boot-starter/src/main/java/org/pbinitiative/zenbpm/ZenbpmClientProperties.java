package org.pbinitiative.zenbpm;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@ConfigurationProperties("zenbpm")
public class ZenbpmClientProperties {

    // rest
    private String restUrl = "http://localhost:8080/v1";
    private boolean restLoggingEnabled = true;

    // grpc
    private String grpcHost = "localhost";
    private int grpcPort = 9090;
    private boolean grpcPlaintext = true;
    private boolean grpcLoggingEnabled = true;
    // Use seconds for unitless values instead of Spring Boot's millisecond default.
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration grpcKeepAliveTime = Duration.ofMinutes(5);
    @DurationUnit(ChronoUnit.SECONDS)
    private Duration grpcKeepAliveTimeout = Duration.ofSeconds(20);
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

    public Duration getGrpcKeepAliveTime() {
        return grpcKeepAliveTime;
    }

    public void setGrpcKeepAliveTime(Duration grpcKeepAliveTime) {
        this.grpcKeepAliveTime = grpcKeepAliveTime;
    }

    public Duration getGrpcKeepAliveTimeout() {
        return grpcKeepAliveTimeout;
    }

    public void setGrpcKeepAliveTimeout(Duration grpcKeepAliveTimeout) {
        this.grpcKeepAliveTimeout = grpcKeepAliveTimeout;
    }

    public boolean isJobWorkerEnabled() {
        return jobWorkerEnabled;
    }

    public void setJobWorkerEnabled(boolean jobWorkerEnabled) {
        this.jobWorkerEnabled = jobWorkerEnabled;
    }

}
