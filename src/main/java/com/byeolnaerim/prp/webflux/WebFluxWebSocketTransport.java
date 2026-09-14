package com.byeolnaerim.prp.webflux;

import com.byeolnaerim.prp.error.ConnectionLostException;
import com.byeolnaerim.prp.transport.LaneRequirements;
import com.byeolnaerim.prp.transport.ReactiveTransport;
import com.byeolnaerim.prp.transport.TransportCloseEvent;
import com.byeolnaerim.prp.transport.TransportConnection;
import com.byeolnaerim.prp.transport.TransportDescription;
import com.byeolnaerim.prp.transport.TransportLane;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public final class WebFluxWebSocketTransport implements ReactiveTransport {
    private final WebSocketSession session;
    private final Sinks.Many<byte[]> outbound = Sinks.many().unicast().onBackpressureBuffer(new ArrayBlockingQueue<>(256));
    private final CompletableFuture<TransportCloseEvent> closed = new CompletableFuture<>();
    private final AtomicBoolean connected = new AtomicBoolean();
    private final AtomicBoolean laneOpened = new AtomicBoolean();
    private final AtomicBoolean ended = new AtomicBoolean();
    private final Flux<byte[]> incoming;

    public WebFluxWebSocketTransport(WebSocketSession session) {
        this.session = java.util.Objects.requireNonNull(session);
        this.incoming = session.receive().map(message -> {
            if (message.getType() != WebSocketMessage.Type.BINARY) throw new ConnectionLostException("PRP WebSocket carrier accepts binary frames only.");
            DataBuffer payload = message.getPayload();
            byte[] bytes = new byte[payload.readableByteCount()];
            payload.read(bytes);
            return bytes;
        }).doOnError(this::fail).doFinally(signal -> finish(null, "carrier-ended"));
    }

    @Override public String id() { return "webflux-websocket"; }

    @Override
    public CompletionStage<TransportConnection> connect() {
        if (!connected.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("WebFlux WebSocket transport can only connect once."));
        return CompletableFuture.completedFuture(new Connection());
    }

    public Mono<Void> outbound() {
        return session.send(outbound.asFlux().map(bytes -> session.binaryMessage(factory -> factory.wrap(bytes))));
    }

    public CompletionStage<TransportCloseEvent> closed() { return closed; }

    private void fail(Throwable error) {
        if (!ended.compareAndSet(false, true)) return;
        outbound.tryEmitError(error);
        closed.completeExceptionally(error);
    }

    private void finish(Integer code, String reason) {
        if (!ended.compareAndSet(false, true)) return;
        outbound.tryEmitComplete();
        closed.complete(new TransportCloseEvent(code, reason));
    }

    private final class Connection implements TransportConnection {
        @Override public TransportDescription description() { return new TransportDescription("webflux-websocket", List.of("reliable", "ordered", "message-boundaries")); }

        @Override
        public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
            if (!"reliable".equals(requirements.reliability()) || !"ordered".equals(requirements.ordering())) return CompletableFuture.failedFuture(new IllegalArgumentException("WebFlux WebSocket exposes a reliable ordered lane."));
            if (!laneOpened.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("WebFlux WebSocket connection exposes one lane."));
            return CompletableFuture.completedFuture(new TransportLane() {
                @Override public String id() { return "webflux-websocket:0"; }
                @Override public java.util.concurrent.Flow.Publisher<byte[]> incoming() { return JdkFlowAdapter.publisherToFlowPublisher(incoming); }
                @Override public CompletionStage<Void> write(byte[] frame) {
                    Sinks.EmitResult result = outbound.tryEmitNext(frame.clone());
                    if (result.isFailure()) return CompletableFuture.failedFuture(new ConnectionLostException("WebFlux WebSocket outbound queue failure: " + result));
                    return CompletableFuture.completedFuture(null);
                }
                @Override public CompletionStage<Void> close(String reason) { return Connection.this.close(null, reason); }
            });
        }

        @Override public CompletionStage<TransportCloseEvent> closed() { return closed; }

        @Override
        public CompletionStage<Void> close(Integer code, String reason) {
            finish(code, reason == null ? "closed" : reason);
            CloseStatus status = code == null ? CloseStatus.NORMAL : new CloseStatus(code, reason == null ? "" : reason);
            return session.close(status).toFuture();
        }
    }
}
