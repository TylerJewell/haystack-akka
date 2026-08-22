package io.akka.haystack.domain;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** SPEC-001 R11: a NORMAL socket accepts at most one connection. */
class PipelineGraphTest {

  private static ComponentSpec source(String name) {
    return new ComponentSpec(name, Set.of("value"), inputs -> Map.of("value", 1));
  }

  @Test
  void secondSenderOnNormalSocketFailsAtBuildTime() {
    ComponentSpec receiver =
        new ComponentSpec("receiver", Set.of("value"), inputs -> Map.of("value", inputs.get("value")))
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    assertThatThrownBy(
            () ->
                new PipelineGraph(
                    Map.of("a", source("a"), "b", source("b"), "receiver", receiver),
                    List.of(
                        new Connection("a", "value", "receiver", "value"),
                        new Connection("b", "value", "receiver", "value"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("only LAZY_VARIADIC or GREEDY_VARIADIC");
  }
}
