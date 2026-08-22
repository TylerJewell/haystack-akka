package io.akka.haystack.domain;

import java.util.List;

/**
 * Readiness rules for a single component, ported from {@code core/pipeline/component_checks.py}.
 * Every method here is a pure function of the graph and the current {@link RunState} — nothing
 * is mutated.
 */
final class ComponentChecks {

  private ComponentChecks() {}

  /**
   * SPEC-001 R5/R6: has this one socket received everything it needs to fire? Only meaningful
   * for {@code GREEDY_VARIADIC} (any real delivery) and {@code NORMAL} (its one real delivery).
   *
   * <p>There is deliberately no {@code LAZY_VARIADIC} case: "every expected sender has
   * delivered" is a real, checkable question (used to order the values handed to the
   * component — see {@code Scheduler.consumeInputs}), but it is not what decides whether a
   * lazy variadic socket blocks a component from running at all. SPEC-001 R4/§4.4: readiness
   * for a lazy variadic socket only ever needs ANY one real delivery, so callers branch on
   * {@link SocketKind#LAZY_VARIADIC} before reaching this method, the same way the source's
   * {@code are_all_sockets_ready} OR-clause makes its own "all senders" check redundant there.
   */
  static boolean hasSocketReceivedAllInputs(InputSocket socket, List<Delivery> arrivals) {
    if (arrivals.isEmpty()) {
      return false;
    }
    return switch (socket.kind()) {
      case GREEDY_VARIADIC -> arrivals.stream().anyMatch(Delivery::isReal);
      case NORMAL -> arrivals.get(arrivals.size() - 1).isReal();
      case LAZY_VARIADIC -> throw new IllegalArgumentException("caller must branch on LAZY_VARIADIC before here");
    };
  }

  private static boolean anySocketInputReceived(List<Delivery> arrivals) {
    return arrivals.stream().anyMatch(Delivery::isReal);
  }

  /**
   * Are all of a component's sockets ready? When {@code onlyMandatory} is true, only
   * mandatory sockets are checked (this is the gate {@code canComponentRun} uses); otherwise
   * every socket that has at least one incoming connection is checked (used for the
   * {@code HIGHEST} priority check).
   */
  static boolean areAllSocketsReady(
      ComponentSpec spec, PipelineGraph graph, RunState state, boolean onlyMandatory) {
    for (InputSocket socket : spec.inputSockets().values()) {
      boolean hasConnection = !graph.incoming(spec.name(), socket.name()).isEmpty();
      if (onlyMandatory) {
        if (!socket.mandatory()) {
          continue;
        }
      } else if (!hasConnection && !socket.mandatory()) {
        continue;
      }
      List<Delivery> arrivals = state.deliveries(spec.name(), socket.name());
      boolean ready =
          socket.kind() == SocketKind.LAZY_VARIADIC
              ? anySocketInputReceived(arrivals)
              : hasSocketReceivedAllInputs(socket, arrivals);
      if (!ready) {
        return false;
      }
    }
    return true;
  }

  static boolean isAnyGreedySocketReady(ComponentSpec spec, RunState state) {
    return spec.inputSockets().values().stream()
        .filter(s -> s.kind() == SocketKind.GREEDY_VARIADIC)
        .anyMatch(s -> anySocketInputReceived(state.deliveries(spec.name(), s.name())));
  }

  private static boolean anyPredecessorsProvidedInput(ComponentSpec spec, RunState state) {
    return spec.inputSockets().values().stream()
        .flatMap(s -> state.deliveries(spec.name(), s.name()).stream())
        .anyMatch(d -> d.sender() != null && d.isReal());
  }

  private static boolean hasUserInput(ComponentSpec spec, RunState state) {
    return spec.inputSockets().values().stream()
        .flatMap(s -> state.deliveries(spec.name(), s.name()).stream())
        .anyMatch(d -> d.sender() == null && d.isReal());
  }

  static boolean hasAnyTrigger(ComponentSpec spec, PipelineGraph graph, RunState state) {
    if (anyPredecessorsProvidedInput(spec, state)) {
      return true;
    }
    if (hasUserInput(spec, state) && state.visits(spec.name()) == 0) {
      return true;
    }
    return graph.hasNoInputSockets(spec.name()) && state.visits(spec.name()) == 0;
  }

  static boolean canComponentRun(ComponentSpec spec, PipelineGraph graph, RunState state) {
    return areAllSocketsReady(spec, graph, state, true) && hasAnyTrigger(spec, graph, state);
  }

  /** Every predecessor component has already run at least once. A scheduling nicety only —
   * it decides READY vs DEFER, never whether a component may run at all. */
  static boolean allPredecessorsExecuted(ComponentSpec spec, PipelineGraph graph, RunState state) {
    return graph.predecessorsOf(spec.name()).stream().allMatch(p -> state.visits(p) > 0);
  }

  static Priority calculatePriority(ComponentSpec spec, PipelineGraph graph, RunState state) {
    if (!canComponentRun(spec, graph, state)) {
      return Priority.BLOCKED;
    }
    if (isAnyGreedySocketReady(spec, state) && areAllSocketsReady(spec, graph, state, false)) {
      return Priority.HIGHEST;
    }
    if (allPredecessorsExecuted(spec, graph, state)) {
      return Priority.READY;
    }
    return Priority.DEFER;
  }
}
