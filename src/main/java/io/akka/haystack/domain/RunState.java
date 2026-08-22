package io.akka.haystack.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The mutable state of one {@code Scheduler.run()} call: pending deliveries and visit counts. */
final class RunState {
  private final Map<String, Map<String, List<Delivery>>> pending = new LinkedHashMap<>();
  private final Map<String, Integer> visits = new LinkedHashMap<>();

  RunState(PipelineGraph graph) {
    for (String name : graph.components().keySet()) {
      visits.put(name, 0);
    }
  }

  int visits(String component) {
    return visits.getOrDefault(component, 0);
  }

  void incrementVisits(String component) {
    visits.merge(component, 1, Integer::sum);
  }

  Map<String, Integer> visitsSnapshot() {
    return Map.copyOf(visits);
  }

  List<Delivery> deliveries(String component, String socket) {
    return pending
        .getOrDefault(component, Map.of())
        .getOrDefault(socket, List.of());
  }

  void deliver(String component, String socket, Delivery delivery) {
    pending
        .computeIfAbsent(component, k -> new LinkedHashMap<>())
        .computeIfAbsent(socket, k -> new ArrayList<>())
        .add(delivery);
  }

  void replace(String component, String socket, List<Delivery> newDeliveries) {
    pending
        .computeIfAbsent(component, k -> new LinkedHashMap<>())
        .put(socket, new ArrayList<>(newDeliveries));
  }
}
