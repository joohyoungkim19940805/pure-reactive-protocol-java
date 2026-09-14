package com.byeolnaerim.prp.transport;

import com.byeolnaerim.prp.error.ConnectionLostException;
import com.byeolnaerim.prp.internal.BoundedPublisher;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MemoryTransport implements ReactiveTransport {
    private final Endpoint endpoint;

    private MemoryTransport(Endpoint endpoint) { this.endpoint = endpoint; }

    public static Pair pair() {
        Endpoint left = new Endpoint("memory:left");
        Endpoint right = new Endpoint("memory:right");
        left.peer = right;
        right.peer = left;
        return new Pair(new MemoryTransport(left), new MemoryTransport(right));
    }

    @Override public String id() { return "memory"; }

    @Override
    public CompletionStage<TransportConnection> connect() {
        if (!endpoint.connected.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("Memory transport endpoint can only connect once."));
        return CompletableFuture.completedFuture(new Connection(endpoint));
    }

    public record Pair(MemoryTransport left, MemoryTransport right) {}

    private static final class Endpoint {
        private final String id;
        private final BoundedPublisher<byte[]> reliableIncoming = new BoundedPublisher<>(4096);
        private final BoundedPublisher<byte[]> datagramIncoming = new BoundedPublisher<>(4096);
        private final CompletableFuture<TransportCloseEvent> closed = new CompletableFuture<>();
        private final AtomicBoolean connected = new AtomicBoolean();
        private final AtomicBoolean reliableLaneOpened = new AtomicBoolean();
        private final AtomicBoolean datagramLaneOpened = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();
        private Endpoint peer;

        private Endpoint(String id) { this.id = id; }

        private void end(Integer code, String reason) {
            if (!ended.compareAndSet(false, true)) return;
            reliableIncoming.complete();
            datagramIncoming.complete();
            closed.complete(new TransportCloseEvent(code, reason));
        }
    }

    private static final class Connection implements TransportConnection {
        private final Endpoint endpoint;
        private Connection(Endpoint endpoint) { this.endpoint = endpoint; }

        @Override public TransportDescription description() {
            return new TransportDescription("memory", List.of("reliable", "ordered", "best-effort", "unordered", "message-boundaries", "memory"));
        }

        @Override
        public boolean supportsLane(LaneRequirements requirements) {
            return isReliable(requirements) || isDatagram(requirements);
        }

        @Override
        public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
            if (isReliable(requirements)) {
                if (!endpoint.reliableLaneOpened.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("Memory connection exposes one reliable lane."));
                int maxFrameBytes = requirements.maxFrameBytes();
                return CompletableFuture.completedFuture(new TransportLane() {
                    @Override public String id() { return endpoint.id + ":reliable"; }
                    @Override public int maxFrameBytes() { return maxFrameBytes; }
                    @Override public java.util.concurrent.Flow.Publisher<byte[]> incoming() { return endpoint.reliableIncoming; }
                    @Override public CompletionStage<Void> write(byte[] frame) {
                        if (endpoint.ended.get() || endpoint.peer.ended.get()) return CompletableFuture.failedFuture(new ConnectionLostException());
                        if (frame == null || frame.length > maxFrameBytes) return CompletableFuture.failedFuture(new IllegalArgumentException("Memory reliable frame exceeds lane limit."));
                        if (!endpoint.peer.reliableIncoming.emit(frame.clone())) return CompletableFuture.failedFuture(new ConnectionLostException("Memory transport incoming queue overflow."));
                        return CompletableFuture.completedFuture(null);
                    }
                    @Override public CompletionStage<Void> close(String reason) { return Connection.this.close(null, reason); }
                });
            }
            if (isDatagram(requirements)) {
                if (!endpoint.datagramLaneOpened.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("Memory connection exposes one datagram lane."));
                int maxFrameBytes = requirements.maxFrameBytes();
                return CompletableFuture.completedFuture(new TransportLane() {
                    @Override public String id() { return endpoint.id + ":datagram"; }
                    @Override public int maxFrameBytes() { return maxFrameBytes; }
                    @Override public java.util.concurrent.Flow.Publisher<byte[]> incoming() { return endpoint.datagramIncoming; }
                    @Override public CompletionStage<Void> write(byte[] frame) {
                        if (endpoint.ended.get() || endpoint.peer.ended.get()) return CompletableFuture.failedFuture(new ConnectionLostException());
                        if (frame == null || frame.length > maxFrameBytes) return CompletableFuture.failedFuture(new IllegalArgumentException("Memory datagram exceeds lane limit."));
                        // Best effort: local pressure drops rather than converting datagrams into a reliable queue.
                        endpoint.peer.datagramIncoming.emit(frame.clone());
                        return CompletableFuture.completedFuture(null);
                    }
                    @Override public CompletionStage<Void> close(String reason) { return CompletableFuture.completedFuture(null); }
                });
            }
            return CompletableFuture.failedFuture(new IllegalArgumentException("Memory transport supports reliable/ordered and best-effort/unordered lanes."));
        }

        @Override public CompletionStage<TransportCloseEvent> closed() { return endpoint.closed; }

        @Override
        public CompletionStage<Void> close(Integer code, String reason) {
            endpoint.end(code, reason);
            endpoint.peer.end(code, reason);
            return CompletableFuture.completedFuture(null);
        }

        private static boolean isReliable(LaneRequirements requirements) {
            return "reliable".equals(requirements.reliability()) && "ordered".equals(requirements.ordering());
        }

        private static boolean isDatagram(LaneRequirements requirements) {
            return "best-effort".equals(requirements.reliability()) && "unordered".equals(requirements.ordering());
        }
    }
}
