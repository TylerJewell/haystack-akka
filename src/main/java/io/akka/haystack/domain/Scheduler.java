package io.akka.haystack.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs a {@link PipelineGraph} to completion: one component at a time, recomputing
 * every component's readiness after each call (SPEC-001 R7), fanning outputs out to
 * every wired receiver whether or not the calling component actually returned a value
 * for that receiver (R1-R3), and stopping either because nothing is runnable anymore or
 * because a component has been visited {@code maxRunsPerComponent} times (R9).
 */
public final class Scheduler {

  private Scheduler() {}

  public static RunResult run(
      PipelineGraph graph, Map<String, Map<String, Object>> externalInputs, int maxRunsPerComponent) {
    RunState state = new RunState(graph);
    for (var componentEntry : externalInputs.entrySet()) {
      for (var socketEntry : componentEntry.getValue().entrySet()) {
        state.deliver(componentEntry.getKey(), socketEntry.getKey(), Delivery.real(null, socketEntry.getValue()));
      }
    }

    TopologicalOrder order = TopologicalOrder.of(graph);
    Map<String, Map<String, Object>> outputsByComponent = new LinkedHashMap<>();
    List<String> trace = new ArrayList<>();

    // The queue is a snapshot of every component's priority, in topological order (which
    // doubles as the FIFO tie-break among equal priorities, matching the source's
    // insertion-count tie-break). It is NOT recomputed after every run -- only when it goes
    // "stale" (empty, or its best remaining priority is worse than READY). This is what lets
    // several equally-READY components (e.g. independent sources) all run before a sibling
    // that a greedy socket only just became ready for gets picked -- see question-log row 6:
    // a greedy variadic socket overwrites, so preempting mid-round would silently discard
    // real deliveries the source's own scheduler still lets land first.
    List<QueueEntry> queue = fillQueue(graph, state, order);

    while (!queue.isEmpty()) {
      int minIndex = minPriorityIndex(queue);
      QueueEntry popped = queue.remove(minIndex);

      if (popped.priority() == Priority.BLOCKED) {
        break;
      }

      String chosen;
      if (popped.priority() == Priority.DEFER) {
        List<QueueEntry> deferGroup = new ArrayList<>();
        deferGroup.add(popped);
        queue.removeIf(
            e -> {
              if (e.priority() == Priority.DEFER) {
                deferGroup.add(e);
                return true;
              }
              return false;
            });
        chosen = order.earliest(deferGroup.stream().map(QueueEntry::component).toList());
      } else {
        chosen = popped.component();
      }

      if (state.visits(chosen) >= maxRunsPerComponent) {
        throw new PipelineMaxRunsExceededException(chosen, maxRunsPerComponent, state.visitsSnapshot());
      }

      ComponentSpec spec = graph.components().get(chosen);
      Map<String, Object> callInputs = consumeInputs(chosen, spec, graph, state);
      Map<String, Object> result = spec.function().run(callInputs);
      if (result == null) {
        result = Map.of();
      }
      state.incrementVisits(chosen);
      trace.add(chosen);
      outputsByComponent.put(chosen, result);
      writeOutputs(chosen, result, graph, state);

      boolean stale = queue.isEmpty() || queue.get(minPriorityIndex(queue)).priority().rank > Priority.READY.rank;
      if (stale) {
        queue = fillQueue(graph, state, order);
      }
    }

    return new RunResult(outputsByComponent, state.visitsSnapshot(), trace);
  }

  private record QueueEntry(String component, Priority priority) {}

  private static List<QueueEntry> fillQueue(PipelineGraph graph, RunState state, TopologicalOrder order) {
    List<QueueEntry> queue = new ArrayList<>();
    for (String name : order.orderedNames()) {
      queue.add(new QueueEntry(name, ComponentChecks.calculatePriority(graph.components().get(name), graph, state)));
    }
    return queue;
  }

  private static int minPriorityIndex(List<QueueEntry> queue) {
    int best = 0;
    for (int i = 1; i < queue.size(); i++) {
      if (queue.get(i).priority().rank < queue.get(best).priority().rank) {
        best = i;
      }
    }
    return best;
  }

  /**
   * Resolves each of a component's sockets to the value its function will see, then prunes the
   * run state: every component-sourced delivery is dropped (consumed), and a greedy socket's
   * external deliveries are dropped too, so a greedy external input cannot re-trigger the
   * pipeline forever; every other socket's external deliveries persist across visits.
   */
  private static Map<String, Object> consumeInputs(
      String component, ComponentSpec spec, PipelineGraph graph, RunState state) {
    Map<String, Object> callInputs = new LinkedHashMap<>();

    for (InputSocket socket : spec.inputSockets().values()) {
      List<Delivery> arrivals = state.deliveries(component, socket.name());

      Object value;
      switch (socket.kind()) {
        case NORMAL -> {
          Delivery real = arrivals.stream().filter(Delivery::isReal).reduce((a, b) -> b).orElse(null);
          value = real != null ? real.value() : socket.defaultValue();
        }
        case GREEDY_VARIADIC -> {
          Delivery first = arrivals.stream().filter(Delivery::isReal).findFirst().orElse(null);
          value = first != null ? List.of(first.value()) : socket.defaultValue();
        }
        case LAZY_VARIADIC -> {
          List<Object> ordered = new ArrayList<>();
          for (Connection c : graph.incoming(component, socket.name())) {
            arrivals.stream()
                .filter(d -> d.isReal() && c.fromComponent().equals(d.sender()))
                .findFirst()
                .ifPresent(d -> ordered.add(d.value()));
          }
          value = ordered.isEmpty() && !socket.mandatory() ? socket.defaultValue() : ordered;
        }
        default -> throw new IllegalStateException("Unhandled socket kind: " + socket.kind());
      }
      callInputs.put(socket.name(), value);

      List<Delivery> retained = arrivals.stream()
          .filter(d -> d.sender() == null && socket.kind() != SocketKind.GREEDY_VARIADIC)
          .toList();
      state.replace(component, socket.name(), retained);
    }

    return callInputs;
  }

  /** SPEC-001 R1-R3, R6: fan a component's returned outputs out to every wired receiver. */
  private static void writeOutputs(
      String component, Map<String, Object> result, PipelineGraph graph, RunState state) {
    ComponentSpec spec = graph.components().get(component);
    for (String outputSocket : spec.outputSocketNames()) {
      boolean produced = result.containsKey(outputSocket);
      Delivery delivery = produced ? Delivery.real(component, result.get(outputSocket)) : Delivery.noOutput(component);

      for (Connection c : graph.outgoing(component, outputSocket)) {
        InputSocket receiverSocket = graph.components().get(c.toComponent()).inputSockets().get(c.toSocket());
        if (receiverSocket.kind() == SocketKind.LAZY_VARIADIC) {
          state.deliver(c.toComponent(), c.toSocket(), delivery);
        } else {
          // NORMAL and GREEDY_VARIADIC both overwrite: a real value replaces whatever was
          // pending, and a NoOutputProduced marker never clobbers a real value already
          // pending (SPEC-001 R6). A greedy socket therefore holds at most one pending
          // delivery at a time even when several senders write to it before it is consumed
          // (question-log row 6, corrected by probe_04_greedy_debug.py).
          if (delivery.isReal() || state.deliveries(c.toComponent(), c.toSocket()).isEmpty()) {
            state.replace(c.toComponent(), c.toSocket(), List.of(delivery));
          }
        }
      }
    }
  }
}
