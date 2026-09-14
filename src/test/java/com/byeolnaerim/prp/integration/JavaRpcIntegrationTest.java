package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.*;
import com.byeolnaerim.prp.core.*;
import com.byeolnaerim.prp.profile.rpc.*;
import com.byeolnaerim.prp.transport.MemoryTransport;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class JavaRpcIntegrationTest {
    @Test
    void binaryRpcSupportsUnaryStreamAndChannelWithFragmentation() throws Exception {
        ProtocolLimits limits = new ProtocolLimits(2048, 1024, 64, 128, 512, 1024 * 1024, 2 * 1024 * 1024, 32, 32, 32, 1024 * 1024, 1024 * 1024, 128);
        RpcProfile serverRpc = new RpcProfile(BinaryPayloadCodec.INSTANCE)
            .register("echo", RpcHandlers.<byte[], byte[]>builder(byte[].class).requestResponse((input, context) -> CompletableFuture.completedFuture(input)).build())
            .register("stream", RpcHandlers.<byte[], byte[]>builder(byte[].class).requestStream((input, context) -> publisher(List.of(input, input, input))).build())
            .register("channel", RpcHandlers.<byte[], byte[]>builder(byte[].class).requestChannel((input, context) -> input).build());
        PrpRuntime serverRuntime = PrpRuntime.builder().limits(limits).extension(serverRpc).build();
        PrpRuntime clientRuntime = PrpRuntime.builder().limits(limits).extension(new RpcProfile(BinaryPayloadCodec.INSTANCE)).build();
        MemoryTransport.Pair pair = MemoryTransport.pair();
        CompletionStage<ReactiveSession> server = serverRuntime.accept(pair.right());
        ReactiveSession client = clientRuntime.connect(pair.left()).toCompletableFuture().get(3, TimeUnit.SECONDS);
        server.toCompletableFuture().get(3, TimeUnit.SECONDS);
        RpcPeer rpc = new RpcPeer(client);
        byte[] payload = new byte[256 * 1024];
        for (int index = 0; index < payload.length; index++) payload[index] = (byte) index;
        assertArrayEquals(payload, rpc.requestResponse("echo", payload, byte[].class).toCompletableFuture().get(3, TimeUnit.SECONDS));
        assertEquals(3, collect(rpc.requestStream("stream", new byte[] {1}, byte[].class).toCompletableFuture().get(3, TimeUnit.SECONDS)).size());
        assertEquals(3, collect(rpc.requestChannel("channel", publisher(List.of(new byte[] {1}, new byte[] {2}, new byte[] {3})), byte[].class).toCompletableFuture().get(3, TimeUnit.SECONDS)).size());
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }

    private static <T> Flow.Publisher<T> publisher(List<T> values) {
        return subscriber -> subscriber.onSubscribe(new Flow.Subscription() {
            int index;
            boolean cancelled;
            @Override public void request(long count) {
                while (!cancelled && count-- > 0 && index < values.size()) subscriber.onNext(values.get(index++));
                if (!cancelled && index == values.size()) { cancelled = true; subscriber.onComplete(); }
            }
            @Override public void cancel() { cancelled = true; }
        });
    }

    private static <T> List<T> collect(Flow.Publisher<T> publisher) throws Exception {
        CompletableFuture<List<T>> result = new CompletableFuture<>();
        List<T> values = new ArrayList<>();
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(Long.MAX_VALUE); }
            @Override public void onNext(T item) { values.add(item); }
            @Override public void onError(Throwable throwable) { result.completeExceptionally(throwable); }
            @Override public void onComplete() { result.complete(values); }
        });
        return result.get(3, TimeUnit.SECONDS);
    }
}
