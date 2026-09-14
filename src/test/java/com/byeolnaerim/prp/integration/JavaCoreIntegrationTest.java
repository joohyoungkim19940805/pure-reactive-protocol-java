package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.*;
import com.byeolnaerim.prp.core.*;
import com.byeolnaerim.prp.transport.MemoryTransport;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.Flow;
import org.junit.jupiter.api.Test;

class JavaCoreIntegrationTest {
    @Test
    void javaInitiatorAndAcceptorHandshakeFragmentAndClose() throws Exception {
        ProtocolLimits limits = new ProtocolLimits(1024, 512, 64, 128, 256, 1024 * 1024, 2 * 1024 * 1024, 16, 16, 16, 1024 * 1024, 1024 * 1024, 64);
        PrpRuntime serverRuntime = PrpRuntime.builder()
            .limits(limits)
            .liveness(new LivenessOptions(Duration.ofMillis(200), Duration.ofSeconds(2)))
            .streamAcceptor(new StreamAcceptor() {
                @Override public boolean accepts(ReactiveSession session, ReactiveStream stream) { return stream.attributes().stream().anyMatch(attribute -> "test.echo".equals(attribute.id())); }
                @Override public CompletionStage<Void> handle(ReactiveSession session, ReactiveStream stream) {
                    CompletableFuture<Void> done = new CompletableFuture<>();
                    stream.subscribe(new Flow.Subscriber<>() {
                        private Flow.Subscription subscription;
                        @Override public void onSubscribe(Flow.Subscription subscription) { this.subscription = subscription; subscription.request(1); }
                        @Override public void onNext(StreamMessage item) { stream.send(item.data()).thenCompose(ignored -> stream.complete()).whenComplete((ignored, error) -> { if (error != null) done.completeExceptionally(error); else done.complete(null); }); }
                        @Override public void onError(Throwable throwable) { done.completeExceptionally(throwable); }
                        @Override public void onComplete() {}
                    });
                    return done;
                }
            })
            .build();
        PrpRuntime clientRuntime = PrpRuntime.builder().limits(limits).liveness(new LivenessOptions(Duration.ofMillis(200), Duration.ofSeconds(2))).build();
        MemoryTransport.Pair pair = MemoryTransport.pair();
        CompletionStage<ReactiveSession> serverStage = serverRuntime.accept(pair.right());
        ReactiveSession client = clientRuntime.connect(pair.left()).toCompletableFuture().get(3, TimeUnit.SECONDS);
        ReactiveSession server = serverStage.toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertEquals(SessionState.READY, client.state());
        assertEquals(SessionState.READY, server.state());

        ReactiveStream stream = client.open(List.of(ProtocolAttribute.text("test.echo", "1"))).toCompletableFuture().get(3, TimeUnit.SECONDS);
        CompletableFuture<byte[]> response = new CompletableFuture<>();
        stream.subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(1); }
            @Override public void onNext(StreamMessage item) { response.complete(item.data()); }
            @Override public void onError(Throwable throwable) { response.completeExceptionally(throwable); }
            @Override public void onComplete() {}
        });
        byte[] payload = new byte[64 * 1024];
        for (int index = 0; index < payload.length; index++) payload[index] = (byte) index;
        stream.send(payload).toCompletableFuture().get(3, TimeUnit.SECONDS);
        stream.complete().toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertTrue(Arrays.equals(payload, response.get(3, TimeUnit.SECONDS)));
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertEquals(SessionState.CLOSED, client.state());
    }
}
