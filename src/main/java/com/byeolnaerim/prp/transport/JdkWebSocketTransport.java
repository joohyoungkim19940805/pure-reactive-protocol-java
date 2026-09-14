package com.byeolnaerim.prp.transport;

import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.error.ConnectionLostException;
import com.byeolnaerim.prp.error.TransportUnavailableException;
import com.byeolnaerim.prp.internal.BoundedPublisher;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class JdkWebSocketTransport implements ReactiveTransport {
    private final URI uri;
    private final HttpClient client;

    public JdkWebSocketTransport(URI uri) { this(uri, HttpClient.newHttpClient()); }

    public JdkWebSocketTransport(URI uri, HttpClient client) {
        this.uri = java.util.Objects.requireNonNull(uri);
        this.client = java.util.Objects.requireNonNull(client);
    }

    @Override public String id() { return "websocket"; }

    @Override
    public CompletionStage<TransportConnection> connect() {
        Listener listener = new Listener();
        return client.newWebSocketBuilder().buildAsync(uri, listener)
            .handle((socket, error) -> {
                if (error != null) throw new TransportUnavailableException("WebSocket connection failed: " + error.getMessage(), error);
                listener.socket = socket;
                return new Connection(socket, listener);
            });
    }

    private static final class Listener implements WebSocket.Listener {
        private final BoundedPublisher<byte[]> incoming = new BoundedPublisher<>(256);
        private final CompletableFuture<TransportCloseEvent> closed = new CompletableFuture<>();
        private final AtomicInteger maxFrameBytes = new AtomicInteger(ProtocolLimits.BOOTSTRAP.maxFrameBytes());
        private final ByteArrayOutputStream message = new ByteArrayOutputStream();
        private final AtomicBoolean ended = new AtomicBoolean();
        private volatile WebSocket socket;

        @Override public void onOpen(WebSocket webSocket) { webSocket.request(1); }

        @Override
        public synchronized CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
            byte[] chunk = new byte[data.remaining()];
            data.get(chunk);
            if (message.size() + chunk.length > maxFrameBytes.get()) {
                fail(new ConnectionLostException("WebSocket PRP frame exceeds the admitted frame bound."));
                webSocket.abort();
                return CompletableFuture.completedFuture(null);
            }
            message.writeBytes(chunk);
            if (last) {
                byte[] frame = message.toByteArray();
                message.reset();
                if (!incoming.emit(frame)) {
                    fail(new ConnectionLostException("WebSocket PRP incoming queue overflow."));
                    webSocket.abort();
                    return CompletableFuture.completedFuture(null);
                }
            }
            webSocket.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            fail(new ConnectionLostException("PRP WebSocket accepts binary messages only."));
            webSocket.abort();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            if (ended.compareAndSet(false, true)) {
                incoming.complete();
                closed.complete(new TransportCloseEvent(statusCode, reason));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override public void onError(WebSocket webSocket, Throwable error) { fail(error); }

        private void fail(Throwable error) {
            if (!ended.compareAndSet(false, true)) return;
            incoming.fail(error);
            closed.completeExceptionally(error);
        }
    }

    private static final class Connection implements TransportConnection {
        private final WebSocket socket;
        private final Listener listener;
        private final AtomicBoolean laneOpened = new AtomicBoolean();
        private Connection(WebSocket socket, Listener listener) { this.socket = socket; this.listener = listener; }

        @Override public TransportDescription description() { return new TransportDescription("websocket", List.of("reliable", "ordered", "message-boundaries")); }

        @Override
        public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
            if (!"reliable".equals(requirements.reliability()) || !"ordered".equals(requirements.ordering())) return CompletableFuture.failedFuture(new IllegalArgumentException("WebSocket exposes a reliable ordered lane."));
            if (!laneOpened.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("WebSocket connection exposes one lane."));
            listener.maxFrameBytes.set(Math.max(requirements.maxFrameBytes(), ProtocolLimits.BOOTSTRAP.maxFrameBytes()));
            return CompletableFuture.completedFuture(new TransportLane() {
                @Override public String id() { return "websocket:0"; }
                @Override public java.util.concurrent.Flow.Publisher<byte[]> incoming() { return listener.incoming; }
                @Override public CompletionStage<Void> write(byte[] frame) {
                    if (socket.isOutputClosed()) return CompletableFuture.failedFuture(new ConnectionLostException("WebSocket is not open."));
                    return socket.sendBinary(ByteBuffer.wrap(frame.clone()), true).thenApply(ignored -> null);
                }
                @Override public CompletionStage<Void> close(String reason) { return Connection.this.close(WebSocket.NORMAL_CLOSURE, reason); }
            });
        }

        @Override public CompletionStage<TransportCloseEvent> closed() { return listener.closed; }

        @Override
        public CompletionStage<Void> close(Integer code, String reason) {
            int status = code == null ? WebSocket.NORMAL_CLOSURE : code;
            if (socket.isOutputClosed()) return CompletableFuture.completedFuture(null);
            String safeReason = reason == null ? "" : reason;
            if (safeReason.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 123) safeReason = "closed";
            return socket.sendClose(status, safeReason).handle((ignored, error) -> null);
        }
    }
}
