package com.byeolnaerim.prp.profile.rpc;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.ReactiveStream;
import com.byeolnaerim.prp.StreamMessage;
import com.byeolnaerim.prp.error.PrpException;
import com.byeolnaerim.prp.internal.FlowSupport;
import com.byeolnaerim.prp.internal.InternalAccess;
import com.byeolnaerim.prp.internal.Stages;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;

public final class RpcPeer implements AutoCloseable {
    private final ReactiveSession session;
    private final RpcSessionState state;
    private final Set<String> ownedTargets = ConcurrentHashMap.newKeySet();

    public RpcPeer(ReactiveSession session) {
        this.session = java.util.Objects.requireNonNull(session);
        if (!session.supports(RpcProfile.CAPABILITY_ID)) throw new RpcCapabilityException("RPC/1 capability was not negotiated: " + RpcProfile.CAPABILITY_ID);
        this.state = InternalAccess.of(session).attachment(RpcSessionState.class);
        if (state == null) throw new PrpException("RPC/1 implementation is not attached to this session.", "RPC_PROFILE_NOT_ATTACHED");
    }

    public ReactiveSession session() { return session; }
    public PayloadCodec codec() { return state.codec; }

    public <I, O> RpcPeer register(String target, RpcHandlers<I, O> handlers) {
        RpcProfile.validateTarget(target);
        state.handlers.put(target, handlers);
        ownedTargets.add(target);
        return this;
    }

    public void unregister(String target) {
        if (!ownedTargets.remove(target)) return;
        state.handlers.remove(target);
    }

    public <I, O> CompletionStage<O> requestResponse(String target, I input, Class<O> responseType) {
        RpcProfile.validateTarget(target);
        return session.open(RpcProfile.profileAttributes(target, "unary")).thenCompose(stream ->
            stream.send(state.codec.encode(input))
                .thenCompose(ignored -> stream.complete())
                .thenCompose(ignored -> exactlyOneResponse(stream, responseType))
        );
    }

    public <I> CompletionStage<Void> fireAndForget(String target, I input) {
        RpcProfile.validateTarget(target);
        return session.open(RpcProfile.profileAttributes(target, "notify")).thenCompose(stream ->
            stream.send(state.codec.encode(input)).thenCompose(ignored -> stream.complete())
        );
    }

    public <I, O> CompletionStage<Flow.Publisher<O>> requestStream(String target, I input, Class<O> responseType) {
        RpcProfile.validateTarget(target);
        return session.open(RpcProfile.profileAttributes(target, "server-stream")).thenCompose(stream ->
            stream.send(state.codec.encode(input)).thenCompose(ignored -> stream.complete()).thenApply(ignored -> decoded(stream, responseType))
        );
    }

    public <I, O> CompletionStage<Flow.Publisher<O>> requestChannel(String target, Flow.Publisher<I> input, Class<O> responseType) {
        RpcProfile.validateTarget(target);
        return session.open(RpcProfile.profileAttributes(target, "duplex")).thenApply(stream -> {
            FlowSupport.pump(input, value -> stream.send(state.codec.encode(value)), stream::complete).whenComplete((ignored, error) -> {
                if (error != null && !stream.closed()) stream.fail(Stages.unwrap(error));
            });
            return decoded(stream, responseType);
        });
    }

    private <O> CompletionStage<O> exactlyOneResponse(ReactiveStream stream, Class<O> responseType) {
        return FlowSupport.exactlyOne(decoded(stream, responseType)).exceptionallyCompose(error -> {
            if (!stream.closed()) return stream.cancel("Unary RPC response did not contain exactly one item.").thenCompose(ignored -> CompletableFuture.failedFuture(Stages.unwrap(error)));
            return CompletableFuture.failedFuture(Stages.unwrap(error));
        });
    }

    private <O> Flow.Publisher<O> decoded(ReactiveStream stream, Class<O> responseType) {
        return FlowSupport.map(stream, message -> decodeMessage(message, responseType));
    }

    private <O> O decodeMessage(StreamMessage message, Class<O> responseType) {
        for (var attribute : message.attributes()) if (attribute.required()) throw new PrpException("RPC/1 does not understand required DATA attribute " + attribute.id() + ".", "RPC_REQUIRED_ATTRIBUTE_UNSUPPORTED");
        return state.codec.decode(message.data(), responseType);
    }

    @Override
    public void close() {
        for (String target : Set.copyOf(ownedTargets)) unregister(target);
    }
}
