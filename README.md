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

This repo uses [mise ](https://mise.jdx.dev/) (`mise.toml`) to pin the local Java and Maven versions used by CI.

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
RELEASE_TAG=v1.4.0 mise run set-release-version
mise run verify-release
```

The release workflow downloads backend OpenAPI/proto sources before running these Maven steps. Git tags keep the `v` prefix, while Maven artifact versions are published without it.

## Releasing

Releases are triggered by the ZenBPM release orchestrator with `workflow_dispatch` input `version` set to the backend release tag, for example `v1.4.0`.

The workflow downloads `openapi/api.yaml` and `pkg/zenclient/proto/zenbpm.proto` from the matching `pbinitiative/zenbpm` tag, sets Maven artifact versions from that tag without the `v` prefix, commits the generated release inputs, tags this repository, creates a GitHub Release, and publishes artifacts to Maven Central under `org.pbinitiative.zenbpm`.

Required repository secrets: `APP_ID_ZENBPM_RELEASE`, `APP_PRIVATE_KEY_ZENBPM_RELEASE`, `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_TOKEN`, `MAVEN_GPG_PRIVATE_KEY`, and `MAVEN_GPG_PASSPHRASE`.

## Getting started

Add the starter to your application and the core client as needed.

Maven:
```xml
<dependency>
  <groupId>org.pbinitiative.zenbpm</groupId>
  <artifactId>zenbpm-spring-boot-starter</artifactId>
  <version>${project.version}</version>
</dependency>
<dependency>
  <groupId>org.pbinitiative.zenbpm</groupId>
  <artifactId>zenbpm-client-core</artifactId>
  <version>${project.version}</version>
</dependency>
```

Configure connection settings in application.yml 

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

Feel free to open issues or pull requests if you find bugs or want new features.
