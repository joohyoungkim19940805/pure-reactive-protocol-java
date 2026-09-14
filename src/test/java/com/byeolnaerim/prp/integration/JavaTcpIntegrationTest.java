package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionState;
import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.profile.rpc.BinaryPayloadCodec;
import com.byeolnaerim.prp.profile.rpc.RpcHandlers;
import com.byeolnaerim.prp.profile.rpc.RpcPeer;
import com.byeolnaerim.prp.profile.rpc.RpcProfile;
import com.byeolnaerim.prp.transport.JdkTcpTransport;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class JavaTcpIntegrationTest {
    @Test
    void actualTcpUsesPrpLengthFramingAndFragmentsLogicalItem() throws Exception {
        ProtocolLimits limits = new ProtocolLimits(2048, 1024, 64, 128, 512, 1024 * 1024, 2 * 1024 * 1024, 32, 32, 32, 1024 * 1024, 1024 * 1024, 128);
        PrpRuntime serverRuntime = PrpRuntime.builder()
            .limits(limits)
            .extension(new RpcProfile(BinaryPayloadCodec.INSTANCE)
                .register("tcp.echo", RpcHandlers.<byte[], byte[]>builder(byte[].class)
                    .requestResponse((input, context) -> CompletableFuture.completedFuture(input))
                    .build()))
            .build();
        PrpRuntime clientRuntime = PrpRuntime.builder().limits(limits).extension(new RpcProfile(BinaryPayloadCodec.INSTANCE)).build();

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            CompletableFuture<ReactiveSession> accepted = new CompletableFuture<>();
            Thread.ofVirtual().start(() -> {
                try {
                    serverRuntime.accept(JdkTcpTransport.accepted(serverSocket.accept()))
                        .whenComplete((session, error) -> { if (error != null) accepted.completeExceptionally(error); else accepted.complete(session); });
                } catch (Throwable error) { accepted.completeExceptionally(error); }
            });

            ReactiveSession client = clientRuntime.connect(new JdkTcpTransport("127.0.0.1", serverSocket.getLocalPort())).toCompletableFuture().get(3, TimeUnit.SECONDS);
            ReactiveSession server = accepted.get(3, TimeUnit.SECONDS);
            assertEquals(SessionState.READY, client.state());
            assertEquals(SessionState.READY, server.state());

            byte[] payload = new byte[128 * 1024];
            for (int index = 0; index < payload.length; index++) payload[index] = (byte) (index * 31);
            byte[] echo = new RpcPeer(client).requestResponse("tcp.echo", payload, byte[].class).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertTrue(Arrays.equals(payload, echo));

            client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertEquals(SessionState.CLOSED, client.state());
        }
    }
}
