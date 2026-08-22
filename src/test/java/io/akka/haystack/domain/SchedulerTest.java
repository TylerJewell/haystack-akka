package io.akka.haystack.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Exercises SPEC-001's rules R1-R11 the same way {@code haystack-port/probes/probe_01.py} and
 * {@code probe_02.py} exercised the real deepset-ai/haystack engine — same shapes, same
 * assertions, this time against the port.
 */
class SchedulerTest {

  private static ComponentSpec constant(String name, Object value) {
    return new ComponentSpec(name, Set.of("value"), inputs -> Map.of("value", value));
  }

  @Test
  void untakenBranchNeverRuns() {
    ComponentSpec router =
        new ComponentSpec("router", Set.of("even", "odd"), inputs -> {
              long v = ((Number) inputs.get("value")).longValue();
              return v % 2 == 0 ? Map.of("even", v) : Map.of("odd", v);
            })
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    List<String> oddRan = new java.util.ArrayList<>();
    ComponentSpec evenSink =
        new ComponentSpec("evenSink", Set.of("out"), inputs -> Map.of("out", inputs.get("value")))
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));
    ComponentSpec oddSink =
        new ComponentSpec("oddSink", Set.of("out"), inputs -> {
              oddRan.add("ran");
              return Map.of("out", inputs.get("value"));
            })
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("router", router, "evenSink", evenSink, "oddSink", oddSink),
            List.of(
                new Connection("router", "even", "evenSink", "value"),
                new Connection("router", "odd", "oddSink", "value")));

    RunResult result = Scheduler.run(graph, Map.of("router", Map.of("value", 4L)), 100);

    assertThat(result.outputsByComponent().get("evenSink").get("out")).isEqualTo(4L);
    assertThat(result.outputsByComponent()).doesNotContainKey("oddSink");
    assertThat(oddRan).isEmpty();
  }

  @Test
  void lazyVariadicWaitsForEverySenderInConnectionOrder() {
    ComponentSpec a = constant("a", 10.0);
    ComponentSpec b = constant("b", 20.0);
    ComponentSpec c = constant("c", 30.0);
    ComponentSpec joiner =
        new ComponentSpec("joiner", Set.of("total", "order"), inputs -> {
              @SuppressWarnings("unchecked")
              List<Object> values = (List<Object>) inputs.get("values");
              double total = values.stream().mapToDouble(v -> (double) v).sum();
              return Map.of("total", total, "order", List.copyOf(values));
            })
            .withInput(InputSocket.mandatory("values", SocketKind.LAZY_VARIADIC));

    // Connected c, a, b -- deliberately NOT alphabetical -- while a, b and c (being
    // independent sources with no edges between them) actually RUN in alphabetical order
    // (R8's tie-break). If the port build the values list from arrival order instead of
    // connection order, this test would see [10, 20, 30] instead of [30, 10, 20].
    PipelineGraph graph =
        new PipelineGraph(
            Map.of("a", a, "b", b, "c", c, "joiner", joiner),
            List.of(
                new Connection("c", "value", "joiner", "values"),
                new Connection("a", "value", "joiner", "values"),
                new Connection("b", "value", "joiner", "values")));

    RunResult result = Scheduler.run(graph, Map.of(), 100);

    assertThat(result.outputsByComponent().get("joiner").get("total")).isEqualTo(60.0);
    assertThat(result.outputsByComponent().get("joiner").get("order")).isEqualTo(List.of(30.0, 10.0, 20.0));
  }

  /**
   * A mandatory lazy variadic socket is not blocked once it has received just ONE real
   * delivery, not all of them (`are_all_sockets_ready`'s extra clause,
   * `component_checks.py:104-106`, alongside `has_socket_received_all_inputs`) -- "waits for
   * every sender" (R4) describes what the component is handed once it finally runs, not a
   * precondition for being scheduled at all. So when one of a lazy variadic socket's expected
   * senders is permanently gated behind an untaken branch and will never fire, the receiver
   * does not block forever the way a normal/greedy socket's sole predecessor blocking would
   * (R2, R3) -- it runs anyway, with whatever partial set of values is actually pending.
   * Verified against the real deepset-ai/haystack engine in `haystack-port/probes/`: an
   * identical shape there returns `{"total": 1.0}`, not a pipeline that never completes.
   */
  @Test
  void lazyVariadicRunsWithAPartialSetWhenOneOfItsSendersIsPermanentlyGated() {
    ComponentSpec router =
        new ComponentSpec("router", Set.of("taken", "untaken"), inputs -> Map.of("taken", 1.0));
    ComponentSpec always = constant("always", 1.0);
    // 'gated' has exactly one predecessor, router's untaken branch, which never fires --
    // so 'gated' itself never runs, and is never counted as a delivered sender.
    ComponentSpec gated =
        new ComponentSpec("gated", Set.of("value"), inputs -> Map.of("value", inputs.get("value")))
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));
    ComponentSpec joiner =
        new ComponentSpec("joiner", Set.of("total"), inputs -> {
              @SuppressWarnings("unchecked")
              List<Object> values = (List<Object>) inputs.get("values");
              return Map.of("total", values.stream().mapToDouble(v -> (double) v).sum());
            })
            .withInput(InputSocket.mandatory("values", SocketKind.LAZY_VARIADIC));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("router", router, "always", always, "gated", gated, "joiner", joiner),
            List.of(
                new Connection("router", "untaken", "gated", "value"),
                new Connection("always", "value", "joiner", "values"),
                new Connection("gated", "value", "joiner", "values")));

    RunResult result = Scheduler.run(graph, Map.of(), 100);

    assertThat(result.outputsByComponent()).doesNotContainKey("gated");
    assertThat(result.outputsByComponent().get("joiner").get("total")).isEqualTo(1.0);
  }

  @Test
  void greedyVariadicOverwritesSoOnlyTheLastDeliverySurvives() {
    ComponentSpec a = constant("a", 1.0);
    ComponentSpec b = constant("b", 2.0);
    ComponentSpec c = constant("c", 3.0);
    ComponentSpec joiner =
        new ComponentSpec("joiner", Set.of("first"), inputs -> {
              @SuppressWarnings("unchecked")
              List<Object> value = (List<Object>) inputs.get("value");
              return Map.of("first", value.get(0));
            })
            .withInput(InputSocket.mandatory("value", SocketKind.GREEDY_VARIADIC));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("a", a, "b", b, "c", c, "joiner", joiner),
            List.of(
                new Connection("a", "value", "joiner", "value"),
                new Connection("b", "value", "joiner", "value"),
                new Connection("c", "value", "joiner", "value")));

    RunResult result = Scheduler.run(graph, Map.of(), 100);

    // a, b and c are all sources (no predecessors), so all three run before the scheduler
    // re-snapshots readiness (SPEC-001 R7) and picks the now-greedy-ready joiner; each
    // delivery overwrites the last (R5, R6), so the joiner runs exactly once, consuming
    // whichever source ran last in topological/name order -- c.
    assertThat(result.outputsByComponent().get("joiner").get("first")).isEqualTo(3.0);
    assertThat(result.visitsByComponent().get("joiner")).isEqualTo(1);
  }

  @Test
  void loopExitsWhenTheLoopingComponentStopsReturningToTheBackEdge() {
    ComponentSpec seed = constant("seed", 0.0);
    ComponentSpec counter =
        new ComponentSpec("counter", Set.of("loopBack", "done"), inputs -> {
              @SuppressWarnings("unchecked")
              List<Object> value = (List<Object>) inputs.get("value");
              double v = (double) value.get(0) + 1;
              return v < 3 ? Map.of("loopBack", v) : Map.of("done", v);
            })
            .withInput(InputSocket.mandatory("value", SocketKind.GREEDY_VARIADIC));
    ComponentSpec passthrough =
        new ComponentSpec("passthrough", Set.of("value"), inputs -> Map.of("value", inputs.get("value")))
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("seed", seed, "counter", counter, "passthrough", passthrough),
            List.of(
                new Connection("seed", "value", "counter", "value"),
                new Connection("counter", "loopBack", "passthrough", "value"),
                new Connection("passthrough", "value", "counter", "value")));

    RunResult result = Scheduler.run(graph, Map.of(), 100);

    assertThat(result.outputsByComponent().get("counter").get("done")).isEqualTo(3.0);
    assertThat(result.visitsByComponent().get("counter")).isEqualTo(3);
  }

  @Test
  void runawayLoopHitsTheVisitCapAndFails() {
    ComponentSpec a =
        new ComponentSpec("a", Set.of("value"), inputs -> {
              @SuppressWarnings("unchecked")
              List<Object> value = (List<Object>) inputs.get("value");
              return Map.of("value", (double) value.get(0) + 1);
            })
            .withInput(InputSocket.mandatory("value", SocketKind.GREEDY_VARIADIC));
    ComponentSpec b =
        new ComponentSpec("b", Set.of("value"), inputs -> Map.of("value", (double) inputs.get("value") + 1))
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("a", a, "b", b),
            List.of(new Connection("a", "value", "b", "value"), new Connection("b", "value", "a", "value")));

    assertThatThrownBy(() -> Scheduler.run(graph, Map.of("a", Map.of("value", 0.0)), 5))
        .isInstanceOf(PipelineMaxRunsExceededException.class)
        .satisfies(
            e -> {
              var ex = (PipelineMaxRunsExceededException) e;
              assertThat(ex.maxRunsPerComponent()).isEqualTo(5);
              assertThat(ex.visitsAtFailure()).containsKeys("a", "b");
            });
  }

  @Test
  void maxRunsExceptionCarriesTheFullVisitMap() {
    ComponentSpec looping =
        new ComponentSpec("looping", Set.of("value"), inputs -> Map.of("value", 1))
            .withInput(InputSocket.mandatory("value", SocketKind.GREEDY_VARIADIC));
    PipelineGraph selfLoop =
        new PipelineGraph(
            Map.of("looping", looping), List.of(new Connection("looping", "value", "looping", "value")));

    assertThatThrownBy(() -> Scheduler.run(selfLoop, Map.of("looping", Map.of("value", 0)), 3))
        .isInstanceOf(PipelineMaxRunsExceededException.class)
        .satisfies(
            e -> {
              var ex = (PipelineMaxRunsExceededException) e;
              assertThat(ex.component()).isEqualTo("looping");
              assertThat(ex.visitsAtFailure()).isEqualTo(Map.of("looping", 3));
            });
  }

  @Test
  void branchOnlyBecomesRunnableAfterItsTriggeringCallReturns() {
    Map<String, Integer> runOrder = new LinkedHashMap<>();
    int[] counter = {0};

    ComponentSpec source =
        new ComponentSpec("source", Set.of("value"), inputs -> {
              runOrder.put("source", counter[0]++);
              return Map.of("value", 1);
            });
    ComponentSpec receiver =
        new ComponentSpec("receiver", Set.of("value"), inputs -> {
              runOrder.put("receiver", counter[0]++);
              return Map.of("value", inputs.get("value"));
            })
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("source", source, "receiver", receiver),
            List.of(new Connection("source", "value", "receiver", "value")));

    Scheduler.run(graph, Map.of(), 100);

    assertThat(runOrder.get("source")).isLessThan(runOrder.get("receiver"));
  }

  /**
   * SPEC-001 R6: a NORMAL socket keeps only its most recently written real value, and a
   * NoOutputProduced marker never clobbers a real value already pending.
   *
   * <p>{@code flap} self-loops on a greedy {@code tick} socket and, on the same visit, writes
   * to {@code sink}'s NORMAL {@code value} socket -- a real value on visits 1 and 2, nothing at
   * all (a marker) on visits 3 and 4, then stops looping. Because {@code flap} precedes
   * {@code sink} in topological order (there is an edge {@code flap -> sink}), the tie-break
   * (R8) always lets {@code flap} run again before {@code sink} is picked, so by the time
   * {@code sink} finally runs, its NORMAL socket has been written four times: 1, 2, marker,
   * marker. If overwrite-with-real and marker-never-clobbers both hold, {@code sink} sees 2.
   */
  @Test
  void realValueOverwritesPendingRealValueNoOutputNeverDoes() {
    // flap withholds 'value' on visits 1-2 (sink stays not-ready -- an empty pending list
    // is unaffected either way), writes the one real value on visit 3 (sink becomes READY
    // and is queued to run next), then withholds 'value' again on visit 4 while still
    // looping, and stops on visit 5. Because sink was already queued as READY after visit
    // 3, it is not re-examined before visit 4's marker lands (SPEC-001 R7 -- the queue is
    // only refilled when it goes stale, and READY does not count as stale) and runs
    // immediately after visit 5 using whatever is then pending. If a marker were allowed
    // to clobber the real value 3 sitting there since visit 3, sink would run with nothing
    // real pending at all instead.
    ComponentSpec flap =
        new ComponentSpec("flap", Set.of("tick", "value"), inputs -> {
              @SuppressWarnings("unchecked")
              List<Object> tick = (List<Object>) inputs.get("tick");
              int v = (int) tick.get(0) + 1;
              if (v >= 5) {
                return Map.of(); // stop: no more tick, no more value
              }
              if (v == 3) {
                return Map.of("tick", v, "value", v); // the only real 'value' this run
              }
              return Map.of("tick", v); // keep looping, withhold 'value'
            })
            .withInput(InputSocket.mandatory("tick", SocketKind.GREEDY_VARIADIC));
    ComponentSpec sink =
        new ComponentSpec("sink", Set.of("out"), inputs -> Map.of("out", inputs.get("value")))
            .withInput(InputSocket.mandatory("value", SocketKind.NORMAL));

    PipelineGraph graph =
        new PipelineGraph(
            Map.of("flap", flap, "sink", sink),
            List.of(
                new Connection("flap", "tick", "flap", "tick"),
                new Connection("flap", "value", "sink", "value")));

    RunResult result = Scheduler.run(graph, Map.of("flap", Map.of("tick", 0)), 100);

    assertThat(result.outputsByComponent().get("sink").get("out")).isEqualTo(3);
    assertThat(result.visitsByComponent().get("flap")).isEqualTo(5);
    assertThat(result.visitsByComponent().get("sink")).isEqualTo(1);
  }
}
