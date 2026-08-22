package io.akka.haystack.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SPEC-001 R8: every component in one cycle shares a single rank. */
class TopologicalOrderTest {

  private static ComponentSpec pass(String name) {
    return new ComponentSpec(name, Set.of("value"), inputs -> Map.of("value", inputs.get("value")))
        .withInput(InputSocket.optional("value", SocketKind.GREEDY_VARIADIC, null));
  }

  @Test
  void cycleMembersShareOneRank() {
    // seed -> a -> b -> a (a,b form a cycle) -> c (downstream of the cycle)
    PipelineGraph graph =
        new PipelineGraph(
            Map.of("seed", pass("seed"), "a", pass("a"), "b", pass("b"), "c", pass("c")),
            List.of(
                new Connection("seed", "value", "a", "value"),
                new Connection("a", "value", "b", "value"),
                new Connection("b", "value", "a", "value"),
                new Connection("b", "value", "c", "value")));

    TopologicalOrder order = TopologicalOrder.of(graph);
    List<String> names = order.orderedNames();

    int rankA = names.indexOf("a");
    int rankB = names.indexOf("b");
    int rankSeed = names.indexOf("seed");
    int rankC = names.indexOf("c");

    // a and b are members of the same cycle, so nothing outside the cycle can be ordered
    // strictly between them: they must be adjacent in the order.
    assertThat(Math.abs(rankA - rankB)).isEqualTo(1);
    assertThat(rankSeed).isLessThan(Math.min(rankA, rankB));
    assertThat(rankC).isGreaterThan(Math.max(rankA, rankB));
  }
}
