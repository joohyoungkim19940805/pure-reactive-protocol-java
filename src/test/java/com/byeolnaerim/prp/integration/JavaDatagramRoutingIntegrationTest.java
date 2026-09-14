package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionDatagram;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.profile.datagram.DatagramPeer;
import com.byeolnaerim.prp.profile.datagram.DatagramRoutingProfile;
import com.byeolnaerim.prp.transport.MemoryTransport;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class JavaDatagramRoutingIntegrationTest {
    @Test
    void routedDatagramsUseNegotiatedNumericRoutesWithoutStealingRawDatagrams() throws Exception {
        MemoryTransport.Pair pair = MemoryTransport.pair();
        PrpRuntime clientRuntime = PrpRuntime.builder()
            .extension(new DatagramRoutingProfile("player.position", "player.aim", "client.only"))
            .build();
        PrpRuntime serverRuntime = PrpRuntime.builder()
            .extension(new DatagramRoutingProfile("player.aim", "player.position", "server.only"))
            .build();

        CompletableFuture<ReactiveSession> accepted = serverRuntime.accept(pair.right()).toCompletableFuture();
        ReactiveSession client = clientRuntime.connect(pair.left()).toCompletableFuture().get(3, TimeUnit.SECONDS);
        ReactiveSession server = accepted.get(3, TimeUnit.SECONDS);
        DatagramPeer clientPeer = new DatagramPeer(client);
        DatagramPeer serverPeer = new DatagramPeer(server);

        assertEquals(List.of("player.aim", "player.position"), clientPeer.routes());
        assertEquals(List.of("player.aim", "player.position"), serverPeer.routes());

        CompletableFuture<byte[]> routed = new CompletableFuture<>();
        serverPeer.route("player.position", packet -> {
            routed.complete(packet.data());
            return CompletableFuture.completedFuture(null);
        });
        clientPeer.send("player.position", new byte[] { 7, 8, 9 }).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(new byte[] { 7, 8, 9 }, routed.get(3, TimeUnit.SECONDS));

        CompletableFuture<byte[]> raw = new CompletableFuture<>();
        server.datagrams().subscribe(new Flow.Subscriber<>() {
            @Override public void onSubscribe(Flow.Subscription subscription) { subscription.request(1); }
            @Override public void onNext(SessionDatagram item) { raw.complete(item.data()); }
            @Override public void onError(Throwable throwable) { raw.completeExceptionally(throwable); }
            @Override public void onComplete() { }
        });
        client.sendDatagram(new byte[] { 1, 2, 3 }).toCompletableFuture().get(3, TimeUnit.SECONDS);
        assertArrayEquals(new byte[] { 1, 2, 3 }, raw.get(3, TimeUnit.SECONDS));

        assertTrue(clientPeer.maxPayloadBytes() < client.maxDatagramBytes());
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }
}
