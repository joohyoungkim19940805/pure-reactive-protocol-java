package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionDatagram;
import com.byeolnaerim.prp.SessionState;
import com.byeolnaerim.prp.core.DatagramCodec;
import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.internal.BoundedPublisher;
import com.byeolnaerim.prp.profile.rpc.BinaryPayloadCodec;
import com.byeolnaerim.prp.profile.rpc.RpcHandlers;
import com.byeolnaerim.prp.profile.rpc.RpcPeer;
import com.byeolnaerim.prp.profile.rpc.RpcProfile;
import com.byeolnaerim.prp.transport.WebTransportTransport;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class JavaWebTransportIntegrationTest {
    @Test
    void nativePrpUsesWebTransportByteStreamFramingAndFragmentsLogicalItems() throws Exception {
        EndpointPair pair = EndpointPair.create();
        ProtocolLimits limits = new ProtocolLimits(2048, 1024, 64, 128, 512, 1024 * 1024, 2 * 1024 * 1024, 32, 32, 32, 1024 * 1024, 1024 * 1024, 128);
        PrpRuntime serverRuntime = PrpRuntime.builder()
            .limits(limits)
            .extension(new RpcProfile(BinaryPayloadCodec.INSTANCE)
                .register("webtransport.echo", RpcHandlers.<byte[], byte[]>builder(byte[].class)
                    .requestResponse((input, context) -> CompletableFuture.completedFuture(input))
                    .build()))
            .build();
        PrpRuntime clientRuntime = PrpRuntime.builder()
            .limits(limits)
            .extension(new RpcProfile(BinaryPayloadCodec.INSTANCE))
            .build();

        CompletableFuture<ReactiveSession> accepted = serverRuntime.accept(WebTransportTransport.from(pair.right()))
            .toCompletableFuture();
        ReactiveSession client = clientRuntime.connect(WebTransportTransport.from(pair.left()))
            .toCompletableFuture().get(3, TimeUnit.SECONDS);
        ReactiveSession server = accepted.get(3, TimeUnit.SECONDS);

        assertEquals(SessionState.READY, client.state());
        assertEquals(SessionState.READY, server.state());
        byte[] payload = new byte[128 * 1024];
        for (int index = 0; index < payload.length; index++) payload[index] = (byte) (index * 31);
        assertArrayEquals(payload, new RpcPeer(client)
            .requestResponse("webtransport.echo", payload, byte[].class)
            .toCompletableFuture().get(3, TimeUnit.SECONDS));

        assertTrue(client.supports(DatagramCodec.CAPABILITY_ID));
        assertTrue(server.supports(DatagramCodec.CAPABILITY_ID));
        assertEquals(1196, client.maxDatagramBytes());
        CompletableFuture<byte[]> datagram = new CompletableFuture<>();
        server.datagrams().subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(1); }
            @Override public void onNext(SessionDatagram item) { datagram.complete(item.data()); }
            @Override public void onError(Throwable throwable) { datagram.completeExceptionally(throwable); }
            @Override public void onComplete() { if (!datagram.isDone()) datagram.completeExceptionally(new IllegalStateException("datagram lane closed")); }
        });
        byte[] datagramPayload = new byte[] { 9, 8, 7, 6 };
        client.sendDatagram(datagramPayload).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(datagramPayload, datagram.get(3, TimeUnit.SECONDS));
        assertThrows(java.util.concurrent.ExecutionException.class, () ->
            client.sendDatagram(new byte[client.maxDatagramBytes() + 1]).toCompletableFuture().get(3, TimeUnit.SECONDS));
        assertEquals(SessionState.READY, client.state());

        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertEquals(SessionState.CLOSED, client.state());
    }

    private record EndpointPair(MemoryEndpoint left, MemoryEndpoint right) {
        private static EndpointPair create() {
            MemoryEndpoint left = new MemoryEndpoint();
            MemoryEndpoint right = new MemoryEndpoint();
            left.peer = right;
            right.peer = left;
            return new EndpointPair(left, right);
        }
    }

    private static final class MemoryEndpoint implements WebTransportTransport.Endpoint {
        private final BoundedPublisher<byte[]> incoming = new BoundedPublisher<>(4096);
        private final BoundedPublisher<byte[]> incomingDatagrams = new BoundedPublisher<>(256);
        private final AtomicBoolean ended = new AtomicBoolean();
        private MemoryEndpoint peer;

        @Override public Flow.Publisher<byte[]> incoming() { return incoming; }

        @Override
        public CompletionStage<Void> write(byte[] bytes) {
            if (ended.get() || peer.ended.get()) return CompletableFuture.failedFuture(new IllegalStateException("closed"));
            int first = Math.min(1, bytes.length);
            int second = Math.min(3, bytes.length);
            if (!peer.incoming.emit(Arrays.copyOfRange(bytes, 0, first))
                || (first < second && !peer.incoming.emit(Arrays.copyOfRange(bytes, first, second)))
                || (second < bytes.length && !peer.incoming.emit(Arrays.copyOfRange(bytes, second, bytes.length)))) {
                return CompletableFuture.failedFuture(new IllegalStateException("incoming queue overflow"));
            }
            return CompletableFuture.completedFuture(null);
        }


        @Override
        public WebTransportTransport.DatagramEndpoint datagrams() {
            return new WebTransportTransport.DatagramEndpoint() {
                @Override public Flow.Publisher<byte[]> incoming() { return incomingDatagrams; }
                @Override public int maxDatagramBytes() { return 1200; }
                @Override
                public CompletionStage<Void> write(byte[] bytes) {
                    if (ended.get() || peer.ended.get()) return CompletableFuture.failedFuture(new IllegalStateException("closed"));
                    if (bytes.length > maxDatagramBytes()) return CompletableFuture.failedFuture(new IllegalArgumentException("datagram too large"));
                    peer.incomingDatagrams.emit(bytes.clone());
                    return CompletableFuture.completedFuture(null);
                }
            };
        }

        @Override
        public CompletionStage<Void> close(String reason) {
            end();
            peer.end();
            return CompletableFuture.completedFuture(null);
        }

        private void end() {
            if (!ended.compareAndSet(false, true)) return;
            incoming.complete();
            incomingDatagrams.complete();
        }
    }
}
