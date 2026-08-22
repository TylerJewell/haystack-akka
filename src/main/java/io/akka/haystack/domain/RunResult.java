package io.akka.haystack.domain;

import java.util.List;
import java.util.Map;

/**
 * What one {@code Scheduler.run()} call produced: every component's own returned output map,
 * how many times each component ran, and the order components ran in.
 *
 * <p>Unlike the source, which by default reports only outputs that were not consumed by a
 * receiver (plus any component named in {@code include_outputs_from}), this always reports
 * every component's full output map — a deliberate widening for an HTTP caller inspecting a
 * run from outside, who has no access to the source's Python return value. See the README's
 * list of differences.
 */
public record RunResult(
    Map<String, Map<String, Object>> outputsByComponent,
    Map<String, Integer> visitsByComponent,
    List<String> trace) {}
