package io.akka.haystack.domain;

/**
 * How urgently a component should run next, lower value first. Mirrors haystack's
 * {@code ComponentPriority} (source: {@code core/pipeline/base.py}).
 */
public enum Priority {
  HIGHEST(1),
  READY(2),
  DEFER(3),
  BLOCKED(4);

  public final int rank;

  Priority(int rank) {
    this.rank = rank;
  }
}
