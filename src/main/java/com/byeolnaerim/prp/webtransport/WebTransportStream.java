package com.byeolnaerim.prp.webtransport;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Runtime-neutral WebTransport application stream. */
public interface WebTransportStream {
    boolean isBidirectional();

    long streamId();

    void onData(Consumer<byte[]> handler);

    void onClose(Runnable handler);

    void onError(Consumer<Throwable> handler);

    CompletableFuture<Void> write(byte[] bytes);

    void close();

    /** Resets the stream using a WebTransport application error code. */
    void reset(long errorCode);
}
