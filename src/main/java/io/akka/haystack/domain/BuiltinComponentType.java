package io.akka.haystack.domain;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A small, fixed vocabulary of component behaviours, so a pipeline can be described and run
 * entirely as data over HTTP without accepting arbitrary code. Each type's socket shape is
 * fixed; {@code config} only parameterises its behaviour (a threshold, a constant, an
 * increment).
 */
public enum BuiltinComponentType {
  /** No inputs; emits {@code config.value} once, on the pipeline's first run. */
  CONSTANT {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of();
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("value");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      Object value = config.get("value");
      return inputs -> Map.of("value", value);
    }
  },

  /** Copies {@code value} straight through. */
  PASSTHROUGH {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("value", SocketKind.NORMAL));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("value");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      return inputs -> Map.of("value", inputs.get("value"));
    }
  },

  /** {@code value} plus {@code config.by} (default 1). */
  INCREMENT {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("value", SocketKind.NORMAL));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("value");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      double by = number(config.getOrDefault("by", 1));
      return inputs -> Map.of("value", number(inputs.get("value")) + by);
    }
  },

  /** Routes an even {@code value} to {@code even}, an odd one to {@code odd} (SPEC-001 R1-R3). */
  PARITY_ROUTER {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("value", SocketKind.NORMAL));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("even", "odd");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      return inputs -> {
        long v = (long) number(inputs.get("value"));
        return v % 2 == 0 ? Map.of("even", v) : Map.of("odd", v);
      };
    }
  },

  /** Routes to {@code below} while {@code value < config.threshold}, else {@code atOrAbove}. */
  THRESHOLD_ROUTER {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("value", SocketKind.NORMAL));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("below", "atOrAbove");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      double threshold = number(config.get("threshold"));
      return inputs -> {
        double v = number(inputs.get("value"));
        return v < threshold ? Map.of("below", v) : Map.of("atOrAbove", v);
      };
    }
  },

  /** Sums whatever real deliveries are pending when it runs (SPEC-001 R4, §4.4). */
  SUM_JOINER {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("values", SocketKind.LAZY_VARIADIC));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("total");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      return inputs -> {
        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) inputs.get("values");
        double total = values.stream().mapToDouble(BuiltinComponentType::number).sum();
        return Map.of("total", total);
      };
    }
  },

  /** Fires on the first of any connected sender, ignoring the rest (SPEC-001 R5). */
  FIRST_OF {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("value", SocketKind.GREEDY_VARIADIC));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("value");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      return inputs -> {
        @SuppressWarnings("unchecked")
        List<Object> value = (List<Object>) inputs.get("value");
        return Map.of("value", value.get(0));
      };
    }
  },

  /**
   * Closes a loop: its {@code value} socket is greedy variadic so it can merge a one-shot seed
   * edge with a loop-back edge (SPEC-001 R11). Increments by 1 each visit; loops back below
   * {@code config.limit}, exits ({@code done}) once it reaches it.
   */
  LOOP_UNTIL {
    @Override
    public List<InputSocket> inputSockets(Map<String, Object> config) {
      return List.of(InputSocket.mandatory("value", SocketKind.GREEDY_VARIADIC));
    }

    @Override
    public Set<String> outputSockets(Map<String, Object> config) {
      return Set.of("loopBack", "done");
    }

    @Override
    public ComponentFunction function(Map<String, Object> config) {
      double limit = number(config.get("limit"));
      return inputs -> {
        @SuppressWarnings("unchecked")
        List<Object> value = (List<Object>) inputs.get("value");
        double v = number(value.get(0)) + 1;
        return v < limit ? Map.of("loopBack", v) : Map.of("done", v);
      };
    }
  };

  public abstract List<InputSocket> inputSockets(Map<String, Object> config);

  public abstract Set<String> outputSockets(Map<String, Object> config);

  public abstract ComponentFunction function(Map<String, Object> config);

  static double number(Object o) {
    return ((Number) o).doubleValue();
  }
}
