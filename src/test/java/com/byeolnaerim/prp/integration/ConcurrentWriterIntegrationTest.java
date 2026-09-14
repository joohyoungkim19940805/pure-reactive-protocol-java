package com.byeolnaerim.prp.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.profile.rpc.BinaryPayloadCodec;
import com.byeolnaerim.prp.profile.rpc.RpcHandlers;
import com.byeolnaerim.prp.profile.rpc.RpcPeer;
import com.byeolnaerim.prp.profile.rpc.RpcProfile;
import com.byeolnaerim.prp.transport.MemoryTransport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ConcurrentWriterIntegrationTest {
    @Test
    void concurrentStreamsDoNotRaceSessionSequenceAssignment() throws Exception {
        ProtocolLimits limits = new ProtocolLimits(2048, 1024, 64, 128, 512, 1024 * 1024, 2 * 1024 * 1024, 64, 64, 64, 1024 * 1024, 1024 * 1024, 128);
        PrpRuntime serverRuntime = PrpRuntime.builder().limits(limits).extension(new RpcProfile(BinaryPayloadCodec.INSTANCE)
            .register("echo", RpcHandlers.<byte[], byte[]>builder(byte[].class)
                .requestResponse((input, context) -> CompletableFuture.completedFuture(input))
                .build())).build();
        PrpRuntime clientRuntime = PrpRuntime.builder().limits(limits).extension(new RpcProfile(BinaryPayloadCodec.INSTANCE)).build();
        MemoryTransport.Pair pair = MemoryTransport.pair();
        CompletableFuture<ReactiveSession> server = serverRuntime.accept(pair.right()).toCompletableFuture();
        ReactiveSession client = clientRuntime.connect(pair.left()).toCompletableFuture().get(3, TimeUnit.SECONDS);
        server.get(3, TimeUnit.SECONDS);
        RpcPeer rpc = new RpcPeer(client);

        List<CompletableFuture<byte[]>> calls = new ArrayList<>();
        for (int call = 0; call < 32; call++) {
            byte[] payload = new byte[8 * 1024];
            Arrays.fill(payload, (byte) call);
            calls.add(rpc.requestResponse("echo", payload, byte[].class).toCompletableFuture());
        }
        CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);
        for (int call = 0; call < calls.size(); call++) {
            byte[] expected = new byte[8 * 1024];
            Arrays.fill(expected, (byte) call);
            assertArrayEquals(expected, calls.get(call).get());
        }
        client.close("done").toCompletableFuture().get(3, TimeUnit.SECONDS);
    }
}
