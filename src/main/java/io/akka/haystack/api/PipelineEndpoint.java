package io.akka.haystack.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.javasdk.http.HttpException;
import io.akka.haystack.domain.BuiltinComponentType;
import io.akka.haystack.domain.ComponentSpec;
import io.akka.haystack.domain.Connection;
import io.akka.haystack.domain.PipelineGraph;
import io.akka.haystack.domain.PipelineMaxRunsExceededException;
import io.akka.haystack.domain.RunResult;
import io.akka.haystack.domain.Scheduler;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs one pipeline graph, described entirely as data (SPEC-001): named components built from
 * {@link BuiltinComponentType}, wired by {@code "component.socket"} connection strings, given
 * external inputs, and executed to completion in a single call.
 */
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.ALL))
@HttpEndpoint("/pipelines")
public class PipelineEndpoint {

  public record ComponentRequest(String name, String type, Map<String, Object> config) {}

  public record RunRequest(
      List<ComponentRequest> components,
      List<String> connections,
      Map<String, Map<String, Object>> externalInputs,
      Integer maxRunsPerComponent) {}

  public record RunResponse(
      Map<String, Map<String, Object>> outputsByComponent,
      Map<String, Integer> visitsByComponent,
      List<String> trace) {
    static RunResponse from(RunResult result) {
      return new RunResponse(result.outputsByComponent(), result.visitsByComponent(), result.trace());
    }
  }

  public record MaxRunsExceededResponse(String component, int maxRunsPerComponent, Map<String, Integer> visits) {}

  @Post("/run")
  public RunResponse run(RunRequest request) {
    PipelineGraph graph = toGraph(request);
    Map<String, Map<String, Object>> externalInputs =
        request.externalInputs() == null ? Map.of() : request.externalInputs();
    int maxRuns = request.maxRunsPerComponent() == null ? 100 : request.maxRunsPerComponent();

    try {
      return RunResponse.from(Scheduler.run(graph, externalInputs, maxRuns));
    } catch (PipelineMaxRunsExceededException e) {
      throw HttpException.badRequest(
          "Component '" + e.component() + "' exceeded the maximum number of allowed runs (" + e.maxRunsPerComponent() + ")");
    }
  }

  private static PipelineGraph toGraph(RunRequest request) {
    if (request.components() == null || request.components().isEmpty()) {
      throw HttpException.badRequest("A pipeline needs at least one component");
    }

    Map<String, ComponentSpec> components = new LinkedHashMap<>();
    for (ComponentRequest cr : request.components()) {
      BuiltinComponentType type;
      try {
        type = BuiltinComponentType.valueOf(cr.type().toUpperCase());
      } catch (IllegalArgumentException e) {
        throw HttpException.badRequest("Unknown component type '" + cr.type() + "'");
      }
      Map<String, Object> config = cr.config() == null ? Map.of() : cr.config();
      ComponentSpec spec = new ComponentSpec(cr.name(), type.outputSockets(config), type.function(config));
      type.inputSockets(config).forEach(spec::withInput);
      components.put(cr.name(), spec);
    }

    List<Connection> connections = new ArrayList<>();
    List<String> connectionStrings = request.connections() == null ? List.of() : request.connections();
    for (String raw : connectionStrings) {
      String[] sides = raw.split("->");
      if (sides.length != 2) {
        throw HttpException.badRequest("Connection '" + raw + "' must be of the form 'component.socket -> component.socket'");
      }
      connections.add(parseEndpoint(sides[0].trim(), sides[1].trim()));
    }

    try {
      return new PipelineGraph(components, connections);
    } catch (IllegalArgumentException e) {
      throw HttpException.badRequest(e.getMessage());
    }
  }

  private static Connection parseEndpoint(String from, String to) {
    String[] fromParts = from.split("\\.", 2);
    String[] toParts = to.split("\\.", 2);
    if (fromParts.length != 2 || toParts.length != 2) {
      throw HttpException.badRequest(
          "Connection endpoints must be 'component.socket', got '" + from + "' -> '" + to + "'");
    }
    return new Connection(fromParts[0], fromParts[1], toParts[0], toParts[1]);
  }
}
