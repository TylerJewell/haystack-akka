package io.akka.haystack.domain;

import java.util.Map;

/**
 * Raised when a component that could still run has already reached {@code maxRunsPerComponent}
 * (SPEC-001 R9) — the sole guard against a cycle with no component-level exit condition.
 *
 * <p>Additionally carries every component's visit count at the moment of failure (SPEC-001
 * §4.3) — the source's {@code PipelineMaxComponentRuns} carries only the offending component's
 * name and the cap.
 */
public final class PipelineMaxRunsExceededException extends RuntimeException {
  private final String component;
  private final int maxRunsPerComponent;
  private final Map<String, Integer> visitsAtFailure;

  public PipelineMaxRunsExceededException(
      String component, int maxRunsPerComponent, Map<String, Integer> visitsAtFailure) {
    super(
        "Component '"
            + component
            + "' exceeded the maximum number of allowed runs ("
            + maxRunsPerComponent
            + ")");
    this.component = component;
    this.maxRunsPerComponent = maxRunsPerComponent;
    this.visitsAtFailure = Map.copyOf(visitsAtFailure);
  }

  public String component() {
    return component;
  }

  public int maxRunsPerComponent() {
    return maxRunsPerComponent;
  }

  public Map<String, Integer> visitsAtFailure() {
    return visitsAtFailure;
  }
}
