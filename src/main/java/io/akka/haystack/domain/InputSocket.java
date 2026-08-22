package io.akka.haystack.domain;

/**
 * A single named input of a component.
 *
 * @param name the socket's name, unique within its component
 * @param kind how it accepts deliveries — see {@link SocketKind}
 * @param mandatory whether a run cannot fire without it (mutually exclusive with a
 *     default value)
 * @param defaultValue the value used when the socket is optional and nothing arrived;
 *     {@code null} when there is none
 */
public record InputSocket(String name, SocketKind kind, boolean mandatory, Object defaultValue) {

  public InputSocket {
    if (mandatory && defaultValue != null) {
      throw new IllegalArgumentException("Socket '" + name + "' cannot be both mandatory and carry a default value");
    }
  }

  public static InputSocket mandatory(String name, SocketKind kind) {
    return new InputSocket(name, kind, true, null);
  }

  public static InputSocket optional(String name, SocketKind kind, Object defaultValue) {
    return new InputSocket(name, kind, false, defaultValue);
  }
}
