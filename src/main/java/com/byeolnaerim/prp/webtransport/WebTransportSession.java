package com.byeolnaerim.prp.webtransport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/** Runtime-neutral server-side WebTransport session. */
public interface WebTransportSession {
    /** The HTTP/3 CONNECT stream ID that identifies this WebTransport session. */
    long getSessionStreamId();

    /** Request path used to establish the session. */
    String path();

    /** Origin header sent by the peer, or an empty string when absent. */
    String origin();

    /** HTTP authority used to establish the session. */
    default String authority() {
        return "";
    }

    /** Immutable request-header view. Header names are normalized by the HTTP/3 implementation. */
    default Map<String, List<String>> requestHeaders() {
        return Map.of();
    }

    default boolean isOpen() {
        return true;
    }

    /** Whether this WebTransport session exposes unreliable datagrams. */
    default boolean supportsDatagrams() { return false; }

    /** Maximum application bytes that may be sent in one WebTransport datagram payload. */
    default int maxDatagramSize() { return 0; }

    /** Register a datagram consumer. The returned handle removes only this consumer. */
    default AutoCloseable onDatagram(Consumer<byte[]> handler) { return () -> {}; }

    /** Best-effort datagram send. Completion means handed to QUIC, not delivered. */
    default CompletionStage<Void> sendDatagram(byte[] data) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("WebTransport datagrams are unavailable."));
    }

    /** Closes the session and all streams that belong to it. */
    void close();
}
