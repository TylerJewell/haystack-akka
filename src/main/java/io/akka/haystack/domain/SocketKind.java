package io.akka.haystack.domain;

/**
 * How an input socket accepts deliveries from its connected senders.
 *
 * <p>Mirrors the three shapes haystack's {@code InputSocket} distinguishes (source:
 * {@code core/component/types.py}): a normal socket takes exactly one sender, a lazy
 * variadic socket waits for every one of its senders before it is ready, and a greedy
 * variadic socket is ready the moment any one of its senders delivers. Unlike the
 * source, which infers {@code LAZY_VARIADIC} from a repeated connection onto a
 * {@code list[T]}-typed socket, this kind is declared explicitly when the socket is
 * built — see SPEC-001 §4.1.
 */
public enum SocketKind {
  NORMAL,
  LAZY_VARIADIC,
  GREEDY_VARIADIC
}
