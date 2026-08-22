package io.akka.haystack.domain;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * One component: its name, its declared sockets, and the function that runs it.
 *
 * <p>Input sockets are declared with {@link #withInput}; a component with no input
 * sockets at all is a source, triggered once on the pipeline's first run (SPEC-001,
 * question-log row 2's "no wired predecessors" case).
 */
public final class ComponentSpec {
  private final String name;
  private final Map<String, InputSocket> inputSockets = new LinkedHashMap<>();
  private final Set<String> outputSocketNames;
  private final ComponentFunction function;

  public ComponentSpec(String name, Set<String> outputSocketNames, ComponentFunction function) {
    this.name = name;
    this.outputSocketNames = Set.copyOf(outputSocketNames);
    this.function = function;
  }

  public ComponentSpec withInput(InputSocket socket) {
    inputSockets.put(socket.name(), socket);
    return this;
  }

  public String name() {
    return name;
  }

  public Map<String, InputSocket> inputSockets() {
    return inputSockets;
  }

  public Set<String> outputSocketNames() {
    return outputSocketNames;
  }

  public ComponentFunction function() {
    return function;
  }
}
