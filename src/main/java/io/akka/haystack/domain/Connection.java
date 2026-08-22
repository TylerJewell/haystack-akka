package io.akka.haystack.domain;

/** One wire: a component's output socket feeding another component's input socket. */
public record Connection(String fromComponent, String fromSocket, String toComponent, String toSocket) {}
