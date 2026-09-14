package com.byeolnaerim.prp.profile.rpc;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.ReactiveStream;
import com.byeolnaerim.prp.StreamMessage;
import com.byeolnaerim.prp.core.CapabilityDescriptor;
import com.byeolnaerim.prp.core.NegotiatedCapability;
import com.byeolnaerim.prp.core.ProtocolExtension;
import com.byeolnaerim.prp.core.PrpText;
import com.byeolnaerim.prp.error.PrpException;
import com.byeolnaerim.prp.internal.FlowSupport;
import com.byeolnaerim.prp.internal.InternalAccess;
import com.byeolnaerim.prp.internal.SessionInternals;
import com.byeolnaerim.prp.internal.Stages;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.ConcurrentHashMap;

public final class RpcProfile implements ProtocolExtension {
    public static final String PROFILE_ATTRIBUTE = "prp.profile";
    public static final String TARGET_ATTRIBUTE = "prp.rpc.target";
    public static final String PATTERN_ATTRIBUTE = "prp.rpc.pattern";
    public static final String PROFILE_ID = "rpc/1";
    public static final String CAPABILITY_ID = "prp.profile.rpc";
    private static final List<String> OPEN_ATTRIBUTES = List.of(PROFILE_ATTRIBUTE, TARGET_ATTRIBUTE, PATTERN_ATTRIBUTE);

    private final PayloadCodec codec;
    private final Map<String, RpcHandlers<?, ?>> initialHandlers = new ConcurrentHashMap<>();

    public RpcProfile(PayloadCodec codec) {
        this.codec = java.util.Objects.requireNonNull(codec);
        if (codec.id() == null || codec.id().isEmpty()) throw new IllegalArgumentException("RPC codec id must not be empty.");
        PrpText.utf8(codec.id());
    }

    public PayloadCodec codec() { return codec; }

    public <I, O> RpcProfile register(String target, RpcHandlers<I, O> handlers) {
        validateTarget(target);
        initialHandlers.put(target, handlers);
        return this;
    }

    @Override
    public CapabilityDescriptor capability() {
        return new CapabilityDescriptor(CAPABILITY_ID, 1, 1, PrpText.utf8(codec.id()));
    }

    @Override
    public CompletionStage<AutoCloseable> attach(ReactiveSession session) {
        if (!session.supports(CAPABILITY_ID)) return Stages.failed(new RpcCapabilityException("RPC/1 capability was not negotiated: " + CAPABILITY_ID));
        SessionInternals internals = InternalAccess.of(session);
        NegotiatedCapability negotiated = internals.capabilities().get(CAPABILITY_ID);
        if (negotiated == null) return Stages.failed(new RpcCapabilityException("RPC/1 capability was not negotiated: " + CAPABILITY_ID));
        String remoteCodec = PrpText.strictText(negotiated.remote().parameters());
        if (!codec.id().equals(remoteCodec)) return Stages.failed(new RpcCapabilityException("RPC/1 codec mismatch: local=" + codec.id() + ", remote=" + remoteCodec + "."));
        RpcSessionState state = new RpcSessionState(codec);
        state.handlers.putAll(initialHandlers);
        internals.attachment(RpcSessionState.class, state);
        AutoCloseable disposer = internals.addStreamAcceptor(new com.byeolnaerim.prp.StreamAcceptor() {
            @Override public boolean accepts(ReactiveSession ignored, ReactiveStream stream) {
                List<ProtocolAttribute> profiles = attributes(stream.attributes(), PROFILE_ATTRIBUTE);
                return profiles.size() == 1 && PROFILE_ID.equals(PrpText.strictText(profiles.getFirst().value()));
            }
            @Override public CompletionStage<Void> handle(ReactiveSession current, ReactiveStream stream) {
                return handleIncoming(current, state, stream);
            }
        });
        state.acceptorDisposer = disposer;
        return CompletableFuture.completedFuture(() -> {
            try { disposer.close(); } finally {
                state.handlers.clear();
                internals.attachment(RpcSessionState.class, null);
            }
        });
    }

    private CompletionStage<Void> handleIncoming(ReactiveSession session, RpcSessionState state, ReactiveStream stream) {
        try {
            for (ProtocolAttribute attribute : stream.attributes()) {
                if (attribute.required() && !OPEN_ATTRIBUTES.contains(attribute.id())) return stream.fail(new PrpException("RPC/1 does not understand required OPEN attribute " + attribute.id() + ".", "RPC_REQUIRED_ATTRIBUTE_UNSUPPORTED"));
            }
            String profile = singletonText(stream.attributes(), PROFILE_ATTRIBUTE);
            String target = singletonText(stream.attributes(), TARGET_ATTRIBUTE);
            String pattern = singletonText(stream.attributes(), PATTERN_ATTRIBUTE);
            if (!PROFILE_ID.equals(profile) || target == null || target.isEmpty() || !List.of("unary", "notify", "server-stream", "duplex").contains(pattern)) return stream.fail(new PrpException("RPC stream has invalid profile, target, or pattern attributes.", "RPC_INVALID_OPEN"));
            RpcHandlers<?, ?> handlers = state.handlers.get(target);
            if (handlers == null) return stream.fail(new PrpException("No RPC handler is registered for " + target + ".", "RPC_NOT_FOUND"));
            return dispatch(session, state, stream, target, pattern, handlers);
        } catch (Throwable error) {
            return stream.fail(error);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CompletionStage<Void> dispatch(ReactiveSession session, RpcSessionState state, ReactiveStream stream, String target, String pattern, RpcHandlers handlers) {
        RpcContext context = new RpcContext(session, stream, target);
        Flow.Publisher decoded = decoded(stream, state.codec, handlers.inputType);
        return switch (pattern) {
            case "unary" -> {
                if (handlers.requestResponse == null) yield stream.fail(new PrpException("RPC target " + target + " does not support unary requests.", "RPC_PATTERN_UNSUPPORTED"));
                yield FlowSupport.exactlyOne(decoded).thenCompose(input -> {
                    CompletionStage<?> response;
                    try { response = handlers.requestResponse.handle(input, context); }
                    catch (Throwable error) { return stream.fail(error); }
                    return response.thenCompose(value -> stream.send(state.codec.encode(value))).thenCompose(ignored -> stream.complete());
                });
            }
            case "notify" -> {
                if (handlers.fireAndForget == null) yield stream.fail(new PrpException("RPC target " + target + " does not support notifications.", "RPC_PATTERN_UNSUPPORTED"));
                yield FlowSupport.exactlyOne(decoded).thenCompose(input -> {
                    CompletionStage<Void> handled;
                    try { handled = handlers.fireAndForget.handle(input, context); }
                    catch (Throwable error) { return stream.fail(error); }
                    return handled.thenCompose(ignored -> stream.complete());
                });
            }
            case "server-stream" -> {
                if (handlers.requestStream == null) yield stream.fail(new PrpException("RPC target " + target + " does not support server streams.", "RPC_PATTERN_UNSUPPORTED"));
                yield FlowSupport.exactlyOne(decoded).thenCompose(input -> {
                    Flow.Publisher output;
                    try { output = handlers.requestStream.handle(input, context); }
                    catch (Throwable error) { return stream.fail(error); }
                    return FlowSupport.pump(output, value -> stream.send(state.codec.encode(value)), stream::complete);
                });
            }
            case "duplex" -> {
                if (handlers.requestChannel == null) yield stream.fail(new PrpException("RPC target " + target + " does not support duplex streams.", "RPC_PATTERN_UNSUPPORTED"));
                Flow.Publisher output;
                try { output = handlers.requestChannel.handle(decoded, context); }
                catch (Throwable error) { yield stream.fail(error); }
                yield FlowSupport.pump(output, value -> stream.send(state.codec.encode(value)), stream::complete);
            }
            default -> stream.fail(new PrpException("Unsupported RPC pattern " + pattern + ".", "RPC_PATTERN_UNSUPPORTED"));
        };
    }

    private static <T> Flow.Publisher<T> decoded(ReactiveStream stream, PayloadCodec codec, Class<T> type) {
        return FlowSupport.map(stream, message -> decode(message, codec, type));
    }

    private static <T> T decode(StreamMessage message, PayloadCodec codec, Class<T> type) {
        for (ProtocolAttribute attribute : message.attributes()) if (attribute.required()) throw new PrpException("RPC/1 does not understand required DATA attribute " + attribute.id() + ".", "RPC_REQUIRED_ATTRIBUTE_UNSUPPORTED");
        return codec.decode(message.data(), type);
    }

    static List<ProtocolAttribute> profileAttributes(String target, String pattern) {
        validateTarget(target);
        return List.of(
            ProtocolAttribute.requiredText(PROFILE_ATTRIBUTE, PROFILE_ID),
            ProtocolAttribute.requiredText(TARGET_ATTRIBUTE, target),
            ProtocolAttribute.requiredText(PATTERN_ATTRIBUTE, pattern)
        );
    }

    static void validateTarget(String target) {
        if (target == null || target.isEmpty()) throw new IllegalArgumentException("RPC target must not be empty.");
        PrpText.utf8(target);
    }

    private static String singletonText(List<ProtocolAttribute> attributes, String id) {
        List<ProtocolAttribute> matches = attributes(attributes, id);
        if (matches.size() > 1) throw new PrpException("RPC/1 OPEN contains duplicate " + id + " attributes.", "RPC_INVALID_OPEN");
        return matches.isEmpty() ? null : PrpText.strictText(matches.getFirst().value());
    }

    private static List<ProtocolAttribute> attributes(List<ProtocolAttribute> attributes, String id) {
        List<ProtocolAttribute> output = new ArrayList<>();
        for (ProtocolAttribute attribute : attributes) if (id.equals(attribute.id())) output.add(attribute);
        return output;
    }
}
