package com.byeolnaerim.prp.webtransport.kwik;

import com.byeolnaerim.prp.webtransport.WebTransportStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import tech.kwik.core.QuicStream;

final class KwikWebTransportStream implements WebTransportStream {
    private static final int READ_BUFFER_BYTES = 16 * 1024;

    private final QuicStream stream;
    private final InputStream input;
    private final boolean bidirectional;
    private final ExecutorService executor;
    private final Runnable terminalCallback;
    private final Object writeLock = new Object();
    private final AtomicBoolean reading = new AtomicBoolean();
    private final AtomicBoolean terminal = new AtomicBoolean();
    private volatile Consumer<byte[]> dataHandler = ignored -> {};
    private volatile Runnable closeHandler = () -> {};
    private volatile Consumer<Throwable> errorHandler = ignored -> {};
    private volatile Throwable terminalError;

    KwikWebTransportStream(
        QuicStream stream,
        InputStream input,
        boolean bidirectional,
        ExecutorService executor,
        Runnable terminalCallback
    ) {
        this.stream = Objects.requireNonNull(stream, "stream");
        this.input = Objects.requireNonNull(input, "input");
        this.bidirectional = bidirectional;
        this.executor = Objects.requireNonNull(executor, "executor");
        this.terminalCallback = terminalCallback == null ? () -> {} : terminalCallback;
    }

    @Override
    public boolean isBidirectional() {
        return bidirectional;
    }

    @Override
    public long streamId() {
        return stream.getStreamId();
    }

    @Override
    public void onData(Consumer<byte[]> handler) {
        dataHandler = Objects.requireNonNull(handler, "handler");
        startReading();
    }

    @Override
    public void onClose(Runnable handler) {
        closeHandler = Objects.requireNonNull(handler, "handler");
        if (terminal.get() && terminalError == null) safeRun(closeHandler);
    }

    @Override
    public void onError(Consumer<Throwable> handler) {
        errorHandler = Objects.requireNonNull(handler, "handler");
        Throwable error = terminalError;
        if (terminal.get() && error != null) safeError(errorHandler, error);
    }

    @Override
    public CompletableFuture<Void> write(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (!bidirectional) {
            return CompletableFuture.failedFuture(new IllegalStateException("Cannot write to an incoming unidirectional WebTransport stream."));
        }
        if (terminal.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("WebTransport stream is closed."));
        }
        byte[] copy = bytes.clone();
        return CompletableFuture.runAsync(() -> {
            try {
                synchronized (writeLock) {
                    OutputStream output = stream.getOutputStream();
                    output.write(copy);
                    output.flush();
                }
            } catch (IOException error) {
                fail(error);
                throw new java.util.concurrent.CompletionException(error);
            }
        }, executor);
    }

    @Override
    public void close() {
        if (!terminal.compareAndSet(false, true)) return;
        try {
            if (bidirectional) {
                synchronized (writeLock) {
                    stream.getOutputStream().close();
                }
            }
        } catch (IOException ignored) {
        }
        try {
            stream.abortReading(KwikWebTransportProtocol.toHttp3Error(0));
        } catch (Throwable ignored) {
        }
        terminalCallback.run();
        safeRun(closeHandler);
    }

    @Override
    public void reset(long errorCode) {
        if (!terminal.compareAndSet(false, true)) return;
        long wireError = KwikWebTransportProtocol.toHttp3Error(errorCode);
        try {
            stream.abortReading(wireError);
        } catch (Throwable ignored) {
        }
        if (bidirectional) {
            try {
                // The association header was sent by the peer, so this endpoint has no local header bytes
                // that need a non-zero reliable-size guarantee on the reverse sending direction.
                stream.resetStreamAt(wireError, 0);
            } catch (Throwable unsupported) {
                try {
                    stream.resetStream(wireError);
                } catch (Throwable ignored) {
                }
            }
        }
        terminalCallback.run();
        safeRun(closeHandler);
    }

    void terminateWithSessionGone() {
        if (!terminal.compareAndSet(false, true)) return;
        try {
            stream.abortReading(KwikWebTransportProtocol.WT_SESSION_GONE);
        } catch (Throwable ignored) {
        }
        if (bidirectional) {
            try {
                // Incoming client-created bidi streams carry their association header on the peer's
                // sending direction. This endpoint has no local association prefix to preserve.
                stream.resetStreamAt(KwikWebTransportProtocol.WT_SESSION_GONE, 0);
            } catch (Throwable unsupported) {
                try {
                    stream.resetStream(KwikWebTransportProtocol.WT_SESSION_GONE);
                } catch (Throwable ignored) {
                }
            }
        }
        terminalCallback.run();
        safeRun(closeHandler);
    }

    private void startReading() {
        if (!reading.compareAndSet(false, true) || terminal.get()) return;
        executor.execute(() -> {
            byte[] buffer = new byte[READ_BUFFER_BYTES];
            try {
                while (!terminal.get()) {
                    int read = input.read(buffer);
                    if (read < 0) {
                        completeFromPeer();
                        return;
                    }
                    if (read == 0) continue;
                    dataHandler.accept(Arrays.copyOf(buffer, read));
                }
            } catch (Throwable error) {
                if (!terminal.get()) fail(error);
            }
        });
    }

    private void completeFromPeer() {
        if (!terminal.compareAndSet(false, true)) return;
        terminalCallback.run();
        safeRun(closeHandler);
    }

    private void fail(Throwable error) {
        if (!terminal.compareAndSet(false, true)) return;
        terminalError = error;
        terminalCallback.run();
        safeError(errorHandler, error);
    }

    private static void safeRun(Runnable callback) {
        try {
            callback.run();
        } catch (Throwable ignored) {
        }
    }

    private static void safeError(Consumer<Throwable> callback, Throwable error) {
        try {
            callback.accept(error);
        } catch (Throwable ignored) {
        }
    }
}
