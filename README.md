# ZenBPM Java Client

> Spring Boot starter and core REST/gRPC client to interact with the ZenBPM process engine. Includes auto-configuration, OpenAPI-generated REST client, optional gRPC job workers, logging and OpenTelemetry hooks.

## Features

* Project uses JDK 8+ and Spring Boot 2.7+ to support legacy systems, but can be easily adjusted to the newest JDK and Spring versions (see pom.xml).
* Spring Boot auto-configuration (drop-in starter)
* REST client (`ApiClient` + typed APIs generated from OpenAPI)
* gRPC job workers via `@JobWorker` and ZenbpmJobWorkerManager
* OpenTelemetry interceptors for REST and spans for gRPC
* Configurable HTTP/gRPC logging

## Build this project
``mvn clean package``

## Using the library

Artifacts are published to **Maven Central** under the `org.pbinitiative.zenbpm` namespace.

### Add the dependencies

Latest release: **1.3.0**

```xml
<properties>
  <zenbpm.version>1.3.0</zenbpm.version>
</properties>

<!-- Spring Boot starter (includes auto-configuration) -->
<dependency>
  <groupId>org.pbinitiative.zenbpm</groupId>
  <artifactId>zenbpm-spring-boot-starter</artifactId>
  <version>${zenbpm.version}</version>
</dependency>

<!-- Core REST + gRPC client (without Spring auto-configuration) -->
<dependency>
  <groupId>org.pbinitiative.zenbpm</groupId>
  <artifactId>zenbpm-client-core</artifactId>
  <version>${zenbpm.version}</version>
</dependency>

<!-- gRPC transport. gRPC Java splits API stubs from the transport implementation
     on purpose, so this library doesn't pull one in transitively. Pick whichever
     transport fits your runtime (grpc-netty-shaded is the most common). -->
<dependency>
  <groupId>io.grpc</groupId>
  <artifactId>grpc-netty-shaded</artifactId>
  <version>1.78.0</version>
  <scope>runtime</scope>
</dependency>
```

## Configuration

Configure connection settings in `application.yml`.

values shown in `zenbpm` section are defaults.

`logging` section configures logging for rest and grpc clients separately.
 - `DEBUG` levels expose headers of calls and responses.
 - `TRACE` level exposes full request and response bodies. <b>Never use this in production!</b>
```yaml
zenbpm:
  restUrl: http://localhost:8080/v1
  restLoggingEnabled: true
  grpcHost: localhost
  grpcPort: 9090
  grpcPlaintext: true
  grpcLoggingEnabled: true
  jobWorkerEnabled: true

# Disable OpenTelemetry — this is an OpenTelemetry SDK property, not a library property.
otel.sdk.disabled: true

logging:
  level:
    root: INFO
    org.pbinitiative.zenbpm.rest: TRACE
    org.pbinitiative.zenbpm.grpc: DEBUG

```

## Working examples

### 1) Use REST APIs
Inject the provided ZenbpmClientService to obtain the ApiClient, then create a typed API.

```java
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.pbinitiative.zenbpm.rest.ZenbpmClientService;
import org.pbinitiative.zenbpm.client.ApiException;
import org.pbinitiative.zenbpm.client.ApiClient;
import org.pbinitiative.zenbpm.client.api.ProcessDefinitionApi;
import org.pbinitiative.zenbpm.client.api.ProcessInstanceApi;
import org.pbinitiative.zenbpm.client.api.dto.CreateProcessInstanceRequest;
import org.pbinitiative.zenbpm.client.api.dto.ProcessInstance;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Service
public class MyService {
  @Autowired
  private ZenbpmClientService zenbpm;

  public Long deployExampleProcess() throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessDefinitionApi defApi = new ProcessDefinitionApi(apiClient);

    File bpmnFile = new File("path/to/process.bpmn");
    return defApi.createProcessDefinition(bpmnFile).getProcessDefinitionKey();
  }

  public ProcessInstance startMyProcess(Long definitionKey) throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessInstanceApi piApi = new ProcessInstanceApi(apiClient);

    Map<String, Object> vars = new HashMap<>();
    vars.put("orderId", 12345L);

    CreateProcessInstanceRequest req = new CreateProcessInstanceRequest()
        .processDefinitionKey(definitionKey)
        .variables(vars);

    return piApi.createProcessInstance(req);
  }
}
```

Notes:
- Available typed APIs include ProcessDefinitionApi, ProcessInstanceApi, JobApi, MessageApi, etc. Construct them with the provided ApiClient.
- Methods and DTOs come from the generated package `org.pbinitiative.zenbpm.client.api` and `org.pbinitiative.zenbpm.client.api.dto`.

### 2) Register a gRPC job worker
Create a Spring bean with a method annotated by `@JobWorker`. Accepted method signatures:
- no parameters
- one parameter of type `org.pbinitiative.zenbpm.proto.Zenbpm.WaitingJob`
- one parameter of type `org.pbinitiative.zenbpm.grpc.JobContext`
- one parameter of type `Map<String, Object>`

Return value can be any object and will be serialized as variables for job completion. Throwing an exception fails the job.

```java
import org.springframework.stereotype.Component;
import org.pbinitiative.zenbpm.grpc.JobWorker;
import org.pbinitiative.zenbpm.grpc.JobContext;
import java.util.Map;
import java.util.HashMap;

@Component
public class EmailWorker {
  @JobWorker("send-email")
  public Map<String, Object> handleJob(JobContext ctx) {
    Map<String,Object> vars = ctx.getVariables();
    String to = (String) vars.get("email");

    // send email ...

    Map<String, Object> result = new HashMap<>();
    result.put("success", true);
    result.put("message", "Email to " + to + " mocked successfully");
    return result;
  }
}
```

The gRPC worker manager connects on application start if `zenbpm.jobWorkerEnabled` is true.

---

## Releasing a new version

Releases are automated via GitHub Actions (`.github/workflows/release.yaml`):

- **Automatic:** Triggered by a `repository_dispatch` event from the [zenbpm](https://github.com/pbinitiative/zenbpm) repo after a new release is published. The workflow pulls the matching `api.yaml`, builds, deploys to Maven Central, and commits the new `api.yaml` back here.
- **Manual:** For ad-hoc releases and end-to-end testing of the deploy pipeline, run from the repo root:
  ```
  gh workflow run release.yaml --ref <branch> --field release_tag=vX.Y.Z
  ```
  This skips the `api.yaml` pull and deploys whatever is currently checked in.

Required secrets: `MAVEN_GPG_PRIVATE_KEY`, `MAVEN_GPG_PASSPHRASE`, `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_TOKEN`.

Feel free to open issues or pull requests if you find bugs or want new features.

