# ZenBPM Java Client

> Spring Boot starter for ZenBPM, plus a core artifact containing the generated REST APIs and gRPC stubs. Spring auto-configuration, `@JobWorker`, logging, and OpenTelemetry hooks are provided by the starter; gRPC workers also require a `ManagedChannel` provider.

## Features

* Spring Boot auto-configuration (drop-in starter)
* REST client (`ApiClient` plus typed APIs generated from OpenAPI)
* gRPC job workers via `@JobWorker` and `ZenbpmJobWorkerManager`
* OpenTelemetry interceptors for REST and spans for gRPC
* Configurable HTTP/gRPC logging

## Build this project

This repo uses [mise](https://mise.jdx.dev/) (`mise.toml`) to pin the local Java and Maven versions used by CI.

First-time setup:

```bash
mise trust
mise install
```

Build:

```bash
mise run build
```

This runs `mvn -B clean package` with pinned Temurin 17 and Maven, without requiring a system Maven installation.

For release validation, set the Maven artifact version from a release tag before building:

```bash
RELEASE_TAG=v1.5.0 mise run set-release-version
mise run verify-release
```

The release workflow downloads backend OpenAPI/proto sources before running these Maven steps. Git tags keep the `v` prefix, while Maven artifact versions are published without it.

## Releasing

Releases are triggered by the ZenBPM release orchestrator with `workflow_dispatch` input `version` set to the backend release tag, for example `v1.5.0`.

The workflow downloads `openapi/api.yaml` and `pkg/zenclient/proto/zenbpm.proto` from the matching `pbinitiative/zenbpm` tag, sets Maven artifact versions from that tag without the `v` prefix, commits the generated release inputs, tags this repository, creates a GitHub Release, and publishes artifacts to Maven Central under `org.pbinitiative.zenbpm`.

Required repository secrets: `APP_ID_ZENBPM_RELEASE`, `APP_PRIVATE_KEY_ZENBPM_RELEASE`, `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_TOKEN`, `MAVEN_GPG_PRIVATE_KEY`, and `MAVEN_GPG_PASSPHRASE`.

## Getting started

The Java client uses Maven group and Java package prefix `org.pbinitiative.zenbpm`.

### Compatibility

The ZenBPM Java client has been tested on:

- Java 17 and Spring Boot 3
- Java 8 and Spring Boot 2.7

The examples below target Java 17 and Spring Boot 3 and intentionally use modern Java syntax.

### Dependencies

For a minimal Spring Boot application, use the standard Boot starter together with the ZenBPM starter and a gRPC channel provider:

```xml
<parent>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-parent</artifactId>
  <version>3.5.16</version>
  <relativePath/>
</parent>

<properties>
  <java.version>17</java.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter</artifactId>
  </dependency>
  <dependency>
    <groupId>org.pbinitiative.zenbpm</groupId>
    <artifactId>zenbpm-spring-boot-starter</artifactId>
    <version>1.5.0</version>
  </dependency>
  <dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty-shaded</artifactId>
    <version>1.78.0</version>
  </dependency>
</dependencies>
```

Do **not** directly declare `org.pbinitiative.zenbpm:zenbpm-client-core` in a Spring application: `zenbpm-spring-boot-starter` brings `zenbpm-client-core` transitively. Client 1.5.0 supplies the gRPC APIs and stubs but no `ManagedChannel` provider, so `grpc-netty-shaded` is required when using `@JobWorker`; it is not an optional worker transport.

A non-Spring application may instead declare `org.pbinitiative.zenbpm:zenbpm-client-core:1.5.0` to use the generated REST APIs or gRPC stubs, but it must construct and configure the clients itself. gRPC use still requires a channel provider. `@JobWorker` and its auto-configuration are provided by the Spring Boot starter.

### Configuration

This tested `application.yml` uses kebab-case Spring properties and environment overrides for the engine endpoints:

```yaml
zenbpm:
  rest-url: ${ZENBPM_REST_URL:http://localhost:8080/v1}
  rest-logging-enabled: false
  grpc-host: ${ZENBPM_GRPC_HOST:localhost}
  grpc-port: ${ZENBPM_GRPC_PORT:9090}
  # Local development only. Use gRPC TLS and an HTTPS REST URL in other environments.
  grpc-plaintext: true
  grpc-logging-enabled: false
  job-worker-enabled: true

otel:
  sdk:
    disabled: true
```

This sample explicitly disables client logging. Set both logging flags to `false` in production, especially when process variables may contain sensitive data. If REST logging is enabled, OkHttp `BASIC` logging at DEBUG emits request/response lines, not headers or bodies. At TRACE, REST `BODY` logging can expose full HTTP data, and gRPC logging can expose job variables and results.

### REST: deploy and start

Place the example classes in the same package as, or a child package of, your `@SpringBootApplication` class so Spring discovers them. Replace the example `package` declaration with your application's package when needed.

`ZenbpmClientService` supplies the configured `ApiClient`. Deploy a `java.io.File` with `ProcessDefinitionApi`, then start with a business key and variables in `CreateProcessInstanceRequest`:

```java
package com.example.zenbpmverification;

import java.io.File;
import java.util.Map;

import org.pbinitiative.zenbpm.client.ApiClient;
import org.pbinitiative.zenbpm.client.ApiException;
import org.pbinitiative.zenbpm.client.api.ProcessDefinitionApi;
import org.pbinitiative.zenbpm.client.api.ProcessInstanceApi;
import org.pbinitiative.zenbpm.client.api.dto.CreateProcessInstanceRequest;
import org.pbinitiative.zenbpm.client.api.dto.ProcessInstance;
import org.pbinitiative.zenbpm.rest.ZenbpmClientService;
import org.springframework.stereotype.Service;

@Service
public class ZenbpmRestService {

    private final ApiClient apiClient;
    private final ProcessDefinitionApi processDefinitionApi;
    private final ProcessInstanceApi processInstanceApi;

    public ZenbpmRestService(ZenbpmClientService zenbpmClientService) {
        this.apiClient = zenbpmClientService.getApiClient();
        this.processDefinitionApi = new ProcessDefinitionApi(apiClient);
        this.processInstanceApi = new ProcessInstanceApi(apiClient);
    }

    public long deploy(File bpmnFile) throws ApiException {
        return processDefinitionApi.createProcessDefinition(bpmnFile).getProcessDefinitionKey();
    }

    public ProcessInstance start(
            long processDefinitionKey,
            String businessKey,
            Map<String, Object> variables) throws ApiException {
        CreateProcessInstanceRequest request = new CreateProcessInstanceRequest()
                .processDefinitionKey(processDefinitionKey)
                .businessKey(businessKey)
                .variables(variables);

        return processInstanceApi.createProcessInstance(request);
    }

    public ProcessInstance get(long processInstanceKey) throws ApiException {
        return processInstanceApi.getProcessInstance(processInstanceKey);
    }
}
```

### gRPC worker

The worker must be a discovered Spring bean. This example validates that `email` is a nonblank `String` and returns a mock completion:

```java
package com.example.zenbpmverification;

import java.util.Map;

import org.pbinitiative.zenbpm.grpc.JobContext;
import org.pbinitiative.zenbpm.grpc.JobWorker;
import org.springframework.stereotype.Component;

@Component
public class EmailWorker {

    /** Returns a mock confirmation; this worker does not send an actual email. */
    @JobWorker("send-email")
    public Map<String, Object> sendEmail(JobContext context) {
        Object emailValue = context.getVariables().get("email");
        if (!(emailValue instanceof String email) || email.isBlank()) {
            throw new IllegalArgumentException("Job variable 'email' must be a nonblank String");
        }

        return Map.of(
                "emailSent", true,
                "confirmation", "Mock email confirmation for " + email);
    }
}
```

An exception from the method fails the job. The returned map is serialized as job-completion output variables. The worker manager connects only when `zenbpm.job-worker-enabled` is true **and** at least one annotated worker has been discovered.

If the gRPC job stream is interrupted, the worker manager recreates it and restores all job-type subscriptions automatically. Reconnect attempts continue with exponential backoff from 100 ms up to 30 seconds; stopping the Spring application cancels pending attempts and closes the channel.

### Tested flow

The BPMN service task's job type must match the annotation. Declare `xmlns:zenbpm="http://zenbpm.pbinitiative.org/1.0"` on the BPMN definitions and place the task definition and output mappings inside the service task's extension elements:

```xml
<bpmn:serviceTask id="SendEmailTask" name="Send email">
  <bpmn:extensionElements>
    <zenbpm:taskDefinition type="send-email" retries="3"/>
    <zenbpm:ioMapping>
      <zenbpm:output source="=emailSent" target="emailSent"/>
      <zenbpm:output source="=confirmation" target="confirmation"/>
    </zenbpm:ioMapping>
  </bpmn:extensionElements>
</bpmn:serviceTask>
```

Deploy the BPMN `File` and start the instance through `ZenbpmRestService`, passing `Map.of("email", "customer@example.com")` and a business key. The `EmailWorker` then receives and completes the `send-email` job over gRPC; the mappings copy its `emailSent` and `confirmation` outputs into process variables.

---

Feel free to open issues or pull requests if you find bugs or want new features.
