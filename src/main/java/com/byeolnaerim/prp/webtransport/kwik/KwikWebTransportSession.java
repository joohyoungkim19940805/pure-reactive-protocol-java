package com.byeolnaerim.prp.webtransport.kwik;

import com.byeolnaerim.prp.webtransport.WebTransportHandler;
import com.byeolnaerim.prp.webtransport.WebTransportSession;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import tech.kwik.flupke.HttpStream;

final class KwikWebTransportSession implements WebTransportSession {
    private static final long MAX_CAPSULE_BYTES = 1024 * 1024L;

    private final long sessionId;
    private final String path;
    private final String origin;
    private final String authority;
    private final Map<String, List<String>> requestHeaders;
    private final WebTransportHandler handler;
    private final HttpStream connectStream;
    private final KwikHttp3ServerConnection connection;
    private final ExecutorService executor;
    private final int maxBidiStreams;
    private final int maxUniStreams;
    private final Runnable removeCallback;
    private final Map<Long, KwikWebTransportStream> streams = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Consumer<byte[]>> datagramHandlers = new CopyOnWriteArrayList<>();
    private final AtomicInteger bidiStreams = new AtomicInteger();
    private final AtomicInteger uniStreams = new AtomicInteger();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean closedCallbackSent = new AtomicBoolean();

    KwikWebTransportSession(
        long sessionId,
        String path,
        String origin,
        String authority,
        Map<String, List<String>> requestHeaders,
        WebTransportHandler handler,
        HttpStream connectStream,
        KwikHttp3ServerConnection connection,
        ExecutorService executor,
        int maxBidiStreams,
        int maxUniStreams,
        Runnable removeCallback
    ) {
        this.sessionId = sessionId;
        this.path = path;
        this.origin = origin;
        this.authority = authority;
        this.requestHeaders = immutableHeaders(requestHeaders);
        this.handler = Objects.requireNonNull(handler, "handler");
        this.connectStream = Objects.requireNonNull(connectStream, "connectStream");
        this.connection = Objects.requireNonNull(connection, "connection");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.maxBidiStreams = maxBidiStreams;
        this.maxUniStreams = maxUniStreams;
        this.removeCallback = removeCallback;
    }

    @Override public long getSessionStreamId() { return sessionId; }
    @Override public String path() { return path; }
    @Override public String origin() { return origin; }
    @Override public String authority() { return authority; }
    @Override public Map<String, List<String>> requestHeaders() { return requestHeaders; }
    @Override public boolean isOpen() { return open.get(); }

    @Override
    public boolean supportsDatagrams() {
        return connection.canSendWebTransportDatagrams();
    }

    @Override
    public int maxDatagramSize() {
        return supportsDatagrams() ? connection.maxWebTransportDatagramPayload(sessionId) : 0;
    }

    @Override
    public AutoCloseable onDatagram(Consumer<byte[]> handler) {
        Objects.requireNonNull(handler, "handler");
        datagramHandlers.add(handler);
        return () -> datagramHandlers.remove(handler);
    }

    @Override
    public CompletionStage<Void> sendDatagram(byte[] data) {
        if (data == null) return CompletableFuture.failedFuture(new NullPointerException("data"));
        if (!open.get() || !supportsDatagrams()) return CompletableFuture.failedFuture(new IllegalStateException("WebTransport datagrams are unavailable."));
        if (data.length > maxDatagramSize()) return CompletableFuture.failedFuture(new IllegalArgumentException("WebTransport datagram exceeds " + maxDatagramSize() + " bytes."));
        try {
            connection.sendWebTransportDatagram(sessionId, data.clone());
            return CompletableFuture.completedFuture(null);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    void acceptDatagram(byte[] payload) {
        if (!open.get() || !supportsDatagrams()) return;
        byte[] snapshot = payload.clone();
        for (Consumer<byte[]> consumer : datagramHandlers) {
            try { consumer.accept(snapshot.clone()); } catch (Throwable ignored) { }
        }
    }

    void accept(KwikIncomingWebTransportStream incoming) {
        if (!open.get()) { reject(incoming); return; }
        AtomicInteger counter = incoming.bidirectional() ? bidiStreams : uniStreams;
        int limit = incoming.bidirectional() ? maxBidiStreams : maxUniStreams;
        if (counter.incrementAndGet() > limit) {
            counter.decrementAndGet();
            reject(incoming);
            return;
        }

        final KwikWebTransportStream[] holder = new KwikWebTransportStream[1];
        KwikWebTransportStream stream = new KwikWebTransportStream(
            incoming.quicStream(), incoming.input(), incoming.bidirectional(), executor,
            () -> {
                KwikWebTransportStream value = holder[0];
                if (value != null && streams.remove(value.streamId(), value)) counter.decrementAndGet();
            }
        );
        holder[0] = stream;
        streams.put(stream.streamId(), stream);
        executor.execute(() -> {
            try {
                if (open.get()) handler.onIncomingStream(this, stream);
                else stream.close();
            } catch (Throwable error) {
                stream.reset(1);
            }
        });
    }

    void monitorConnectStream() {
        try {
            InputStream input = connectStream.getInputStream();
            while (open.get()) {
                KwikWebTransportProtocol.VarInt type;
                try { type = KwikWebTransportProtocol.readVarInt(input); }
                catch (java.io.EOFException end) { break; }
                KwikWebTransportProtocol.VarInt length = KwikWebTransportProtocol.readVarInt(input);
                if (length.value() > MAX_CAPSULE_BYTES || length.value() > Integer.MAX_VALUE) throw new IllegalStateException("WebTransport CONNECT capsule is too large.");
                byte[] payload = input.readNBytes((int) length.value());
                if (payload.length != (int) length.value()) break;
                // RFC HTTP Datagrams require no context-registration capsule. Unknown capsules are ignored.
            }
        } catch (Throwable ignored) {
        } finally {
            close();
        }
    }


    @Override public void close() { close(true); }
    void abortBeforeEstablished() { close(false, false); }
    private void close(boolean notifyRegistry) { close(notifyRegistry, true); }

    private void close(boolean notifyRegistry, boolean notifyHandler) {
        if (!open.compareAndSet(true, false)) return;
        datagramHandlers.clear();
        for (KwikWebTransportStream stream : streams.values()) stream.terminateWithSessionGone();
        streams.clear();
        bidiStreams.set(0);
        uniStreams.set(0);
        try { OutputStream output = connectStream.getOutputStream(); output.close(); } catch (Throwable ignored) { }
        try { connectStream.abortReading(KwikWebTransportProtocol.WT_SESSION_GONE); } catch (Throwable ignored) { }
        if (notifyRegistry) try { removeCallback.run(); } catch (Throwable ignored) { }
        if (notifyHandler && closedCallbackSent.compareAndSet(false, true)) {
            try { handler.onSessionClosed(this); } catch (Throwable ignored) { }
        }
    }

    private static void reject(KwikIncomingWebTransportStream incoming) {
        long errorCode = KwikWebTransportProtocol.toHttp3Error(1);
        incoming.quicStream().abortReading(errorCode);
        if (incoming.bidirectional()) incoming.quicStream().resetStream(errorCode);
    }

    private static Map<String, List<String>> immutableHeaders(Map<String, List<String>> source) {
        LinkedHashMap<String, List<String>> copy = new LinkedHashMap<>();
        source.forEach((key, value) -> copy.put(key, List.copyOf(value)));
        return Map.copyOf(copy);
    }
}
