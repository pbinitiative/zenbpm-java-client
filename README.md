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

Artifacts are served via **[JitPack](https://jitpack.io)** — no authentication or extra secrets required.

### 1 — Add the JitPack repository to your pom.xml

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>
```

### 2 — Add the dependencies

Latest release: **v1.3.0**

For multi-module projects JitPack uses `com.github.{owner}.{repo}` as the groupId.
The version is the git tag exactly as pushed (e.g. `v1.3.0`).

```xml
<properties>
  <zenbpm.version>v1.3.0</zenbpm.version>
</properties>

<!-- Spring Boot starter (includes auto-configuration) -->
<dependency>
  <groupId>com.github.pbinitiative.zenbpm-java-client</groupId>
  <artifactId>zenbpm-spring-boot-starter</artifactId>
  <version>${zenbpm.version}</version>
</dependency>

<!-- Core REST + gRPC client (without Spring auto-configuration) -->
<dependency>
  <groupId>com.github.pbinitiative.zenbpm-java-client</groupId>
  <artifactId>zenbpm-client-core</artifactId>
  <version>${zenbpm.version}</version>
</dependency>

<!-- gRPC transport – required at runtime -->
<dependency>
  <groupId>io.grpc</groupId>
  <artifactId>grpc-netty-shaded</artifactId>
  <version>1.78.0</version>
  <scope>runtime</scope>
</dependency>
```

JitPack builds the library on the first request for a given version and caches it
thereafter. If a version shows as "unknown" the first resolution may take ~1–2 minutes.

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

otel.sdk.disabled: true
  
logging:
  level:
    root: INFO
    org.zenbpm.rest: TRACE
    org.zenbpm.grpc: DEBUG

```

## Working examples

### 1) Use REST APIs
Inject the provided ZenbpmClientService to obtain the ApiClient, then create a typed API.

```java
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.zenbpm.rest.ZenbpmClientService;
import org.zenbpm.client.ApiException;
import org.zenbpm.client.ApiClient;
import org.zenbpm.client.api.ProcessDefinitionApi;
import org.zenbpm.client.api.ProcessInstanceApi;
import org.zenbpm.client.api.dto.CreateProcessInstanceRequest;

import java.util.HashMap;
import java.util.Map;

@Service
public class MyService {
  @Autowired
  private ZenbpmClientService zenbpm;

  public Long deployExampleProcess() throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessDefinitionApi defApi = new ProcessDefinitionApi(apiClient);

    // Example: create a process definition from a BPMN string (adjust to your endpoint contract)
    String bpmnXml = "<definitions ...>...</definitions>";
    Long definitionKey = defApi.createProcessDefinition(bpmnXml).getProcessDefinitionKey();
    return definitionKey;
  }

  public void startMyProcess() throws ApiException {
    ApiClient apiClient = zenbpm.getApiClient();
    ProcessInstanceApi piApi = new ProcessInstanceApi(apiClient);

    Map<String, Object> vars = new HashMap<>();
    vars.put("orderId", 12345L);

    CreateProcessInstanceRequest req = new CreateProcessInstanceRequest()
        .processDefinitionKey(123456L)
        .variables(vars);

    piApi.createProcessInstance(req);
  }
}
```

Notes:
- Available typed APIs include ProcessDefinitionApi, ProcessInstanceApi, JobApi, MessageApi, etc. Construct them with the provided ApiClient.
- Methods and DTOs come from the generated package `org.zenbpm.client.api` and `org.zenbpm.client.api.dto`.

### 2) Register a gRPC job worker
Create a Spring bean with a method annotated by `@JobWorker`. Accepted method signatures:
- no parameters
- one parameter of type `org.zenbpm.proto.Zenbpm.WaitingJob`
- one parameter of type `org.zenbpm.grpc.JobContext`
- one parameter of type `Map<String, Object>`

Return value can be any object and will be serialized as variables for job completion. Throwing an exception fails the job.

```java
import org.springframework.stereotype.Component;
import org.zenbpm.grpc.JobWorker;
import org.zenbpm.grpc.JobContext;
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

Feel free to open issues or pull requests if you find bugs or want new features.

