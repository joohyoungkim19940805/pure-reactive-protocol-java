package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.*;
import com.byeolnaerim.prp.core.*;
import com.byeolnaerim.prp.profile.datagram.DatagramPeer;
import com.byeolnaerim.prp.profile.datagram.DatagramRoutingProfile;
import com.byeolnaerim.prp.transport.*;
import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class CompactFramingIntegrationTest {
    @Test void negotiatedPeersKeepBootstrapLegacyThenExchangeCompactFragmentsAndCredit() throws Exception {
        var pair = MemoryTransport.pair();
        var left = new RecordingTransport(pair.left()); var right = new RecordingTransport(pair.right());
        var limits = ProtocolLimits.builder().maxFrameBytes(128).maxAttributeBytes(64)
            .maxAttributeIdBytes(32).maxAttributeValueBytes(64).build();
        var accepted = PrpRuntime.builder().compactFrames(true).limits(limits).streamAcceptor(echo()).build()
            .accept(right).toCompletableFuture();
        var client = PrpRuntime.builder().compactFrames(true).limits(limits).build().connect(left)
            .toCompletableFuture().get(3, TimeUnit.SECONDS);
        var server = accepted.get(3, TimeUnit.SECONDS);
        assertTrue(client.supports(CompactFrameCodec.CAPABILITY_ID)); assertTrue(server.supports(CompactFrameCodec.CAPABILITY_ID));
        byte[] payload = new byte[4096]; for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
        assertArrayEquals(payload, exchange(client, payload));
        assertEquals(FrameKind.HELLO, FrameCodec.decode(left.reliable.getFirst(), ProtocolLimits.BOOTSTRAP).kind());
        assertEquals(FrameKind.WELCOME, FrameCodec.decode(right.reliable.getFirst(), ProtocolLimits.BOOTSTRAP).kind());
        assertTrue(left.reliable.size() > 3); assertTrue(right.reliable.size() > 3);
        for (byte[] bytes : left.reliable.subList(1, left.reliable.size())) assertTrue(CompactFrameCodec.isCompact(bytes));
        for (byte[] bytes : right.reliable.subList(1, right.reliable.size())) assertTrue(CompactFrameCodec.isCompact(bytes));
        assertTrue(left.reliable.stream().skip(1).map(bytes -> CompactFrameCodec.decode(bytes, limits))
            .anyMatch(frame -> frame.kind() == FrameKind.FRAGMENT));
        assertTrue(right.reliable.stream().skip(1).map(bytes -> CompactFrameCodec.decode(bytes, limits))
            .anyMatch(frame -> frame.kind() == FrameKind.DEMAND));
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    @Test void eitherLegacyEndpointKeepsAllTrafficInLegacyMode() throws Exception {
        for (boolean compactClient : List.of(false, true)) {
            var pair = MemoryTransport.pair();
            var left = new RecordingTransport(pair.left()); var right = new RecordingTransport(pair.right());
            var accepted = PrpRuntime.builder().compactFrames(!compactClient).streamAcceptor(echo()).build()
                .accept(right).toCompletableFuture();
            var client = PrpRuntime.builder().compactFrames(compactClient).build().connect(left)
                .toCompletableFuture().get(3, TimeUnit.SECONDS);
            var server = accepted.get(3, TimeUnit.SECONDS);
            assertFalse(client.supports(CompactFrameCodec.CAPABILITY_ID)); assertFalse(server.supports(CompactFrameCodec.CAPABILITY_ID));
            assertArrayEquals(new byte[]{1, 2}, exchange(client, new byte[]{1, 2}));
            for (byte[] bytes : left.reliable) assertFalse(CompactFrameCodec.isCompact(bytes));
            for (byte[] bytes : right.reliable) assertFalse(CompactFrameCodec.isCompact(bytes));
            var received = next(server.datagrams());
            client.sendDatagram(new byte[]{3}).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertArrayEquals(new byte[]{3}, received.get(3, TimeUnit.SECONDS).data());
            assertEquals(5, left.datagrams.getFirst().length);
            client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }

    @Test void bufferedHelloNegotiatesCompactAndDatagramsBeforeTheServerSubscribes() throws Exception {
        var pair = MemoryTransport.pair();
        var clientReady = PrpRuntime.builder().compactFrames(true).build().connect(pair.left()).toCompletableFuture();
        var server = PrpRuntime.builder().compactFrames(true).build().accept(pair.right())
            .toCompletableFuture().get(3, TimeUnit.SECONDS);
        var client = clientReady.get(3, TimeUnit.SECONDS);
        assertTrue(client.supports(CompactFrameCodec.CAPABILITY_ID));
        var received = next(server.datagrams());
        client.sendDatagram(new byte[]{4, 5}).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(new byte[]{4, 5}, received.get(3, TimeUnit.SECONDS).data());
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    @Test void compactDatagramsRetainNumericRoutingAndRawPayloadSeparation() throws Exception {
        var pair = MemoryTransport.pair();
        var left = new RecordingTransport(pair.left());
        var runtime = PrpRuntime.builder().compactFrames(true).extension(new DatagramRoutingProfile("world.motion")).build();
        var accepted = runtime.accept(pair.right()).toCompletableFuture();
        var client = runtime.connect(left).toCompletableFuture().get(3, TimeUnit.SECONDS);
        var server = accepted.get(3, TimeUnit.SECONDS);
        var routed = new CompletableFuture<byte[]>();
        new DatagramPeer(server).route("world.motion", packet -> {
            routed.complete(packet.data()); return CompletableFuture.completedFuture(null);
        });
        new DatagramPeer(client).send("world.motion", new byte[]{8, 9}).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(new byte[]{8, 9}, routed.get(3, TimeUnit.SECONDS));
        assertEquals(9, left.datagrams.getFirst().length); // one core byte + six route bytes + two payload bytes
        var raw = next(server.datagrams());
        client.sendDatagram(new byte[]{1, 2}).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(new byte[]{1, 2}, raw.get(3, TimeUnit.SECONDS).data());
        assertEquals(3, left.datagrams.getLast().length);
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    @Test void wrongDatagramModeIsDroppedWithoutPoisoningTheReliableSession() throws Exception {
        var pair = MemoryTransport.pair(); var left = new RecordingTransport(pair.left());
        var accepted = PrpRuntime.builder().compactFrames(true).build().accept(pair.right()).toCompletableFuture();
        var client = PrpRuntime.builder().compactFrames(true).build().connect(left).toCompletableFuture().get(3, TimeUnit.SECONDS);
        var server = accepted.get(3, TimeUnit.SECONDS);
        var received = next(server.datagrams());
        left.datagramLane.write(DatagramCodec.encode(new byte[]{7}, 32)).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertFalse(received.isDone()); assertEquals(SessionState.READY, server.state());
        client.sendDatagram(new byte[]{9}).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(new byte[]{9}, received.get(3, TimeUnit.SECONDS).data());
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    @Test void theNegotiatedReliableModeCannotSwitchBackOrBeInjectedIntoLegacySessions() throws Exception {
        for (boolean compact : List.of(false, true)) {
            var pair = MemoryTransport.pair(); var left = new RecordingTransport(pair.left());
            var accepted = PrpRuntime.builder().compactFrames(compact).build().accept(pair.right()).toCompletableFuture();
            var client = PrpRuntime.builder().compactFrames(compact).build().connect(left).toCompletableFuture().get(3, TimeUnit.SECONDS);
            var server = accepted.get(3, TimeUnit.SECONDS);
            var signal = new ProtocolFrame(FrameKind.SIGNAL, BigInteger.ZERO, BigInteger.TWO, 0, 0, List.of(), new byte[0]);
            byte[] wrong = compact ? FrameCodec.encode(signal, ProtocolLimits.DEFAULT)
                : CompactFrameCodec.encode(signal, ProtocolLimits.DEFAULT);
            left.reliableLane.write(wrong).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(SessionState.DETACHED, server.state());
            client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }

    private static byte[] exchange(ReactiveSession client, byte[] bytes) throws Exception {
        var stream = client.open(List.of(ProtocolAttribute.text("test.echo", "1"))).toCompletableFuture().get(3, TimeUnit.SECONDS);
        var response = next(stream);
        stream.send(bytes).toCompletableFuture().get(3, TimeUnit.SECONDS);
        stream.complete().toCompletableFuture().get(3, TimeUnit.SECONDS);
        return response.get(3, TimeUnit.SECONDS).data();
    }

    private static <T> CompletableFuture<T> next(Flow.Publisher<T> publisher) {
        var result = new CompletableFuture<T>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(1); }
            @Override public void onNext(T item) { result.complete(item); }
            @Override public void onError(Throwable throwable) { result.completeExceptionally(throwable); }
            @Override public void onComplete() {}
        });
        return result;
    }

    private static StreamAcceptor echo() {
        return new StreamAcceptor() {
            @Override public boolean accepts(ReactiveSession session, ReactiveStream stream) {
                return stream.attributes().stream().anyMatch(attribute -> attribute.id().equals("test.echo"));
            }
            @Override public CompletionStage<Void> handle(ReactiveSession session, ReactiveStream stream) {
                var done = new CompletableFuture<Void>();
                stream.subscribe(new Flow.Subscriber<>() {
                    @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(1); }
                    @Override public void onNext(StreamMessage message) {
                        stream.send(message.data()).thenCompose(ignored -> stream.complete()).whenComplete((ignored, error) -> {
                            if (error == null) done.complete(null); else done.completeExceptionally(error);
                        });
                    }
                    @Override public void onError(Throwable throwable) { done.completeExceptionally(throwable); }
                    @Override public void onComplete() {}
                });
                return done;
            }
        };
    }

    private static final class RecordingTransport implements ReactiveTransport {
        final ReactiveTransport delegate;
        final List<byte[]> reliable = new CopyOnWriteArrayList<>(), datagrams = new CopyOnWriteArrayList<>();
        TransportLane reliableLane, datagramLane;
        RecordingTransport(ReactiveTransport delegate) { this.delegate = delegate; }
        @Override public String id() { return delegate.id(); }
        @Override public CompletionStage<TransportConnection> connect() {
            return delegate.connect().thenApply(connection -> new TransportConnection() {
                @Override public TransportDescription description() { return connection.description(); }
                @Override public boolean supportsLane(LaneRequirements requirements) { return connection.supportsLane(requirements); }
                @Override public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
                    return connection.openLane(requirements).thenApply(lane -> {
                        boolean ordered = requirements.ordering().equals("ordered");
                        if (ordered) reliableLane = lane; else datagramLane = lane;
                        return new TransportLane() {
                            @Override public String id() { return lane.id(); }
                            @Override public int maxFrameBytes() { return lane.maxFrameBytes(); }
                            @Override public Flow.Publisher<byte[]> incoming() { return lane.incoming(); }
                            @Override public CompletionStage<Void> write(byte[] frame) {
                                (ordered ? reliable : datagrams).add(frame.clone()); return lane.write(frame);
                            }
                            @Override public CompletionStage<Void> close(String reason) { return lane.close(reason); }
                        };
                    });
                }
                @Override public CompletionStage<TransportCloseEvent> closed() { return connection.closed(); }
                @Override public CompletionStage<Void> close(Integer code, String reason) { return connection.close(code, reason); }
            });
        }
    }
}
