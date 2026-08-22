package io.akka.haystack.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.haystack.api.PipelineEndpoint.ComponentRequest;
import io.akka.haystack.api.PipelineEndpoint.RunRequest;
import io.akka.haystack.api.PipelineEndpoint.RunResponse;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The capability driven the way a caller outside a test would drive it: over HTTP, against a
 * started runtime. {@code SchedulerTest} and friends check the rules directly; this checks that
 * something outside a test can reach them at all (PIPELINE.md step d's reachability question).
 */
public class PipelineEndpointTest extends TestKitSupport {

  private RunResponse run(RunRequest request) {
    return httpClient.POST("/pipelines/run").withRequestBody(request).responseBodyAs(RunResponse.class).invoke().body();
  }

  @Test
  public void anUntakenBranchNeverRunsOverHttp() {
    var request =
        new RunRequest(
            List.of(
                new ComponentRequest("router", "parity_router", Map.of()),
                new ComponentRequest("evenSink", "passthrough", Map.of()),
                new ComponentRequest("oddSink", "passthrough", Map.of())),
            List.of("router.even -> evenSink.value", "router.odd -> oddSink.value"),
            Map.of("router", Map.of("value", 4)),
            null);

    RunResponse response = run(request);

    assertThat(response.outputsByComponent().get("evenSink").get("value")).isEqualTo(4);
    assertThat(response.outputsByComponent()).doesNotContainKey("oddSink");
    assertThat(response.trace()).containsExactly("router", "evenSink");
  }

  @Test
  public void aLoopClosedByASeedAndABackEdgeRunsToItsExitOverHttp() {
    var request =
        new RunRequest(
            List.of(
                new ComponentRequest("seed", "constant", Map.of("value", 0)),
                new ComponentRequest("counter", "loop_until", Map.of("limit", 3)),
                new ComponentRequest("passthrough", "passthrough", Map.of())),
            List.of(
                "seed.value -> counter.value",
                "counter.loopBack -> passthrough.value",
                "passthrough.value -> counter.value"),
            Map.of(),
            null);

    RunResponse response = run(request);

    assertThat(response.outputsByComponent().get("counter").get("done")).isEqualTo(3.0);
    assertThat(response.visitsByComponent().get("counter")).isEqualTo(3);
  }

  @Test
  public void aRunawayLoopReturnsBadRequestNotAServerError() {
    var request =
        new RunRequest(
            List.of(new ComponentRequest("looping", "loop_until", Map.of("limit", 999))),
            List.of("looping.loopBack -> looping.value"),
            Map.of("looping", Map.of("value", 0)),
            5);

    var response = httpClient.POST("/pipelines/run").withRequestBody(request).invoke();

    assertThat(response.httpResponse().status().intValue()).isEqualTo(400);
  }
}
