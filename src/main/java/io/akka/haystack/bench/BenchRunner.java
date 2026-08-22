package io.akka.haystack.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.haystack.domain.BuiltinComponentType;
import io.akka.haystack.domain.ComponentSpec;
import io.akka.haystack.domain.Connection;
import io.akka.haystack.domain.PipelineGraph;
import io.akka.haystack.domain.PipelineMaxRunsExceededException;
import io.akka.haystack.domain.RunResult;
import io.akka.haystack.domain.Scheduler;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The port's side of the benchmark: this rebuild answering every workload in
 * {@code haystack-port/bench/workloads.json}, in the same shape
 * {@code haystack-port/bench/source_run.py} writes for the source side.
 *
 * <pre>
 *   java -cp target/classes:&lt;jackson&gt; io.akka.haystack.bench.BenchRunner &lt;workloads.json&gt; answers
 *   java -cp ...                       io.akka.haystack.bench.BenchRunner &lt;workloads.json&gt; timings
 * </pre>
 */
public final class BenchRunner {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final int WARMUP_REPETITIONS = 20_000;
  private static final int TIMING_REPETITIONS = 20_000;

  private BenchRunner() {}

  public static void main(String[] args) throws IOException {
    var workloads = (ArrayNode) JSON.readTree(Files.readString(Path.of(args[0])));
    var mode = args.length > 1 ? args[1] : "answers";
    if (mode.equals("timings")) {
      System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(timings(workloads)));
    } else {
      System.out.println(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(answers(workloads)));
    }
  }

  private static ObjectNode answers(ArrayNode workloads) {
    var out = JSON.createObjectNode();
    for (var workload : workloads) {
      var name = workload.get("name").asText();
      try {
        var built = build(workload);
        var result = Scheduler.run(built.graph, built.externalInputs, built.maxRunsPerComponent);
        out.set(name, toJson(result));
      } catch (PipelineMaxRunsExceededException | IllegalArgumentException e) {
        var err = JSON.createObjectNode();
        err.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        out.set(name, err);
      }
    }
    return out;
  }

  private static ObjectNode timings(ArrayNode workloads) {
    var out = JSON.createObjectNode();
    for (var workload : workloads) {
      var name = workload.get("name").asText();
      Built built;
      try {
        built = build(workload);
        for (int i = 0; i < WARMUP_REPETITIONS; i++) {
          Scheduler.run(built.graph, built.externalInputs, built.maxRunsPerComponent);
        }
      } catch (RuntimeException e) {
        continue; // an erroring workload has no steady-state timing to report
      }

      long start = System.nanoTime();
      for (int i = 0; i < TIMING_REPETITIONS; i++) {
        Scheduler.run(built.graph, built.externalInputs, built.maxRunsPerComponent);
      }
      long elapsedNs = System.nanoTime() - start;

      var row = JSON.createObjectNode();
      row.put("repetitions", TIMING_REPETITIONS);
      row.put("nsPerOp", elapsedNs / (double) TIMING_REPETITIONS);
      out.set(name, row);
    }
    return out;
  }

  private static ObjectNode toJson(RunResult result) {
    var out = JSON.createObjectNode();
    var outputs = out.putObject("outputsByComponent");
    for (var entry : result.outputsByComponent().entrySet()) {
      var component = outputs.putObject(entry.getKey());
      for (var socketEntry : entry.getValue().entrySet()) {
        component.putPOJO(socketEntry.getKey(), socketEntry.getValue());
      }
    }
    var visits = out.putObject("visitsByComponent");
    for (var entry : result.visitsByComponent().entrySet()) {
      visits.put(entry.getKey(), entry.getValue());
    }
    return out;
  }

  private record Built(PipelineGraph graph, Map<String, Map<String, Object>> externalInputs, int maxRunsPerComponent) {}

  private static Built build(JsonNode workload) {
    Map<String, ComponentSpec> components = new LinkedHashMap<>();
    for (JsonNode cr : workload.get("components")) {
      var type = BuiltinComponentType.valueOf(cr.get("type").asText().toUpperCase());
      Map<String, Object> config = toMap(cr.get("config"));
      var spec = new ComponentSpec(cr.get("name").asText(), type.outputSockets(config), type.function(config));
      type.inputSockets(config).forEach(spec::withInput);
      components.put(cr.get("name").asText(), spec);
    }

    List<Connection> connections = new ArrayList<>();
    if (workload.has("connections")) {
      for (JsonNode c : workload.get("connections")) {
        String[] sides = c.asText().split("->");
        String[] from = sides[0].trim().split("\\.", 2);
        String[] to = sides[1].trim().split("\\.", 2);
        connections.add(new Connection(from[0], from[1], to[0], to[1]));
      }
    }

    Map<String, Map<String, Object>> externalInputs = new LinkedHashMap<>();
    if (workload.has("externalInputs")) {
      Iterator<Map.Entry<String, JsonNode>> fields = workload.get("externalInputs").properties().iterator();
      while (fields.hasNext()) {
        var entry = fields.next();
        externalInputs.put(entry.getKey(), toMap(entry.getValue()));
      }
    }

    int maxRuns = workload.has("maxRunsPerComponent") ? workload.get("maxRunsPerComponent").asInt() : 100;
    return new Built(new PipelineGraph(components, connections), externalInputs, maxRuns);
  }

  private static Map<String, Object> toMap(JsonNode node) {
    Map<String, Object> map = new LinkedHashMap<>();
    if (node == null || node.isNull()) {
      return map;
    }
    Iterator<Map.Entry<String, JsonNode>> fields = node.properties().iterator();
    while (fields.hasNext()) {
      var entry = fields.next();
      map.put(entry.getKey(), JSON.convertValue(entry.getValue(), Object.class));
    }
    return map;
  }
}
