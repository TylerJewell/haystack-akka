package io.akka.haystack.domain;

import java.util.Map;

/**
 * A component's own logic: a pure function from resolved input-socket values to output-socket
 * values.
 *
 * <p>A {@code LAZY_VARIADIC} or {@code GREEDY_VARIADIC} socket's resolved value is a {@code
 * java.util.List<Object>}; every other socket's is the value itself. The returned map need not
 * contain every declared output socket name — a name it omits delivers {@link NoOutputProduced}
 * to that socket's receivers (SPEC-001 R1).
 */
@FunctionalInterface
public interface ComponentFunction {
  Map<String, Object> run(Map<String, Object> inputs);
}
