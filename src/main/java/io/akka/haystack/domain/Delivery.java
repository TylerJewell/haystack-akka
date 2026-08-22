package io.akka.haystack.domain;

/**
 * One pending value on a socket: either a real value from a sender (or the external
 * caller, when {@code sender} is {@code null}), or the "this receiver was wired but its
 * sender did not return a value for it" marker (SPEC-001 R1).
 */
public record Delivery(String sender, Object value, boolean noOutput) {

  public static Delivery real(String sender, Object value) {
    return new Delivery(sender, value, false);
  }

  public static Delivery noOutput(String sender) {
    return new Delivery(sender, null, true);
  }

  public boolean isReal() {
    return !noOutput;
  }
}
