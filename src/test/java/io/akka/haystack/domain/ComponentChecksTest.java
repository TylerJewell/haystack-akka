package io.akka.haystack.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SPEC-001 R2: a {@code NoOutputProduced} delivery is not a trigger. */
class ComponentChecksTest {

  @Test
  void markerDeliveryIsNotATrigger() {
    ComponentSpec upstream = new ComponentSpec("upstream", Set.of("taken", "untaken"), inputs -> Map.of("taken", 1));
    // 'value' is OPTIONAL, not mandatory: the socket-readiness gate (R4/R5/R6) is
    // vacuously satisfied with nothing pending, so whether 'receiver' ever runs depends
    // entirely on hasAnyTrigger -- this isolates R2 from the readiness rules, which a
    // mandatory socket would not (a marker delivery also fails the readiness gate on its
    // own, so a mandatory-socket test cannot tell "trigger ignores markers" apart from
    // "readiness ignores markers").
    ComponentSpec receiver =
        new ComponentSpec("receiver", Set.of("value"), inputs -> Map.of("value", inputs.get("value")))
            .withInput(InputSocket.optional("value", SocketKind.NORMAL, "default"));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("upstream", upstream, "receiver", receiver),
            List.of(new Connection("upstream", "untaken", "receiver", "value")));

    RunResult result = Scheduler.run(graph, Map.of(), 100);

    assertThat(result.trace()).containsExactly("upstream");
    assertThat(result.outputsByComponent()).doesNotContainKey("receiver");
    assertThat(result.visitsByComponent().get("receiver")).isZero();
  }
}
