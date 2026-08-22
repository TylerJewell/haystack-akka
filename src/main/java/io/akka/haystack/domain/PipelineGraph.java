package io.akka.haystack.domain;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A graph of components wired by named sockets. Need not be acyclic (SPEC-001,
 * question-log row 8: the source never checks for or forbids a cycle at build time).
 *
 * <p>Validates SPEC-001 R11 at construction: a {@code NORMAL} socket may have at most
 * one connection into it.
 */
public final class PipelineGraph {
  private final Map<String, ComponentSpec> components;
  private final List<Connection> connections;

  /** Every connection landing on (component, inputSocket), in the order they were added. */
  private final Map<String, Map<String, List<Connection>>> incomingBySocket = new LinkedHashMap<>();

  /** Every connection leaving (component, outputSocket). */
  private final Map<String, Map<String, List<Connection>>> outgoingBySocket = new LinkedHashMap<>();

  /** Every component with at least one edge into this one. */
  private final Map<String, Set<String>> predecessors = new LinkedHashMap<>();

  public PipelineGraph(Map<String, ComponentSpec> components, List<Connection> connections) {
    this.components = Map.copyOf(components);
    this.connections = List.copyOf(connections);

    for (Connection c : this.connections) {
      ComponentSpec toSpec = requireComponent(c.toComponent());
      InputSocket socket = toSpec.inputSockets().get(c.toSocket());
      if (socket == null) {
        throw new IllegalArgumentException(
            "Component '" + c.toComponent() + "' has no input socket '" + c.toSocket() + "'");
      }

      var socketIncoming =
          incomingBySocket.computeIfAbsent(c.toComponent(), k -> new LinkedHashMap<>())
              .computeIfAbsent(c.toSocket(), k -> new ArrayList<>());
      if (socket.kind() == SocketKind.NORMAL && !socketIncoming.isEmpty()) {
        throw new IllegalArgumentException(
            "Component '"
                + c.toComponent()
                + "' cannot accept multiple inputs to '"
                + c.toSocket()
                + "': it is already connected, and only LAZY_VARIADIC or GREEDY_VARIADIC"
                + " sockets accept more than one sender (SPEC-001 R11)");
      }
      socketIncoming.add(c);

      outgoingBySocket
          .computeIfAbsent(c.fromComponent(), k -> new LinkedHashMap<>())
          .computeIfAbsent(c.fromSocket(), k -> new ArrayList<>())
          .add(c);

      predecessors.computeIfAbsent(c.toComponent(), k -> new LinkedHashSet<>()).add(c.fromComponent());
    }
  }

  private ComponentSpec requireComponent(String name) {
    ComponentSpec spec = components.get(name);
    if (spec == null) {
      throw new IllegalArgumentException("Unknown component '" + name + "'");
    }
    return spec;
  }

  public Map<String, ComponentSpec> components() {
    return components;
  }

  public List<Connection> connections() {
    return connections;
  }

  /** Connections feeding (component, inputSocket), in connection order. */
  public List<Connection> incoming(String component, String socket) {
    return incomingBySocket
        .getOrDefault(component, Map.of())
        .getOrDefault(socket, List.of());
  }

  /** Connections leaving (component, outputSocket). */
  public List<Connection> outgoing(String component, String socket) {
    return outgoingBySocket
        .getOrDefault(component, Map.of())
        .getOrDefault(socket, List.of());
  }

  /** Every component with at least one edge into {@code component}. */
  public Set<String> predecessorsOf(String component) {
    return predecessors.getOrDefault(component, Set.of());
  }

  /** True when {@code component} has no wired input sockets at all — a source. */
  public boolean hasNoInputSockets(String component) {
    return requireComponent(component).inputSockets().isEmpty();
  }
}
