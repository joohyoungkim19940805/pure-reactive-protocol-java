package com.byeolnaerim.prp.webtransport;

/**
 * Application callback for a WebTransport session.
 *
 * <p>The transport server owns protocol parsing and lifecycle. Implementations only receive
 * WebTransport sessions and their application streams.</p>
 */
public interface WebTransportHandler {
    /** Called after the HTTP/3 CONNECT request has been accepted. */
    default void onSessionReady(WebTransportSession session) {}

    /** Called for each incoming application stream that belongs to the session. */
    void onIncomingStream(WebTransportSession session, WebTransportStream stream);

    /** Called exactly once when the session is closed. */
    default void onSessionClosed(WebTransportSession session) {}
}
