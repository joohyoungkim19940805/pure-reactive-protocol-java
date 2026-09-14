package com.byeolnaerim.prp.profile.rpc;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class RpcHandlers<I, O> {
    @FunctionalInterface public interface RequestResponse<I, O> { CompletionStage<O> handle(I input, RpcContext context); }
    @FunctionalInterface public interface FireAndForget<I> { CompletionStage<Void> handle(I input, RpcContext context); }
    @FunctionalInterface public interface RequestStream<I, O> { Flow.Publisher<O> handle(I input, RpcContext context); }
    @FunctionalInterface public interface RequestChannel<I, O> { Flow.Publisher<O> handle(Flow.Publisher<I> input, RpcContext context); }

    final Class<I> inputType;
    final RequestResponse<I, O> requestResponse;
    final FireAndForget<I> fireAndForget;
    final RequestStream<I, O> requestStream;
    final RequestChannel<I, O> requestChannel;

    private RpcHandlers(Builder<I, O> builder) {
        inputType = builder.inputType;
        requestResponse = builder.requestResponse;
        fireAndForget = builder.fireAndForget;
        requestStream = builder.requestStream;
        requestChannel = builder.requestChannel;
    }

    public static <I, O> Builder<I, O> builder(Class<I> inputType) { return new Builder<>(inputType); }

    public static final class Builder<I, O> {
        private final Class<I> inputType;
        private RequestResponse<I, O> requestResponse;
        private FireAndForget<I> fireAndForget;
        private RequestStream<I, O> requestStream;
        private RequestChannel<I, O> requestChannel;
        private Builder(Class<I> inputType) { this.inputType = java.util.Objects.requireNonNull(inputType); }
        public Builder<I, O> requestResponse(RequestResponse<I, O> handler) { requestResponse = handler; return this; }
        public Builder<I, O> fireAndForget(FireAndForget<I> handler) { fireAndForget = handler; return this; }
        public Builder<I, O> requestStream(RequestStream<I, O> handler) { requestStream = handler; return this; }
        public Builder<I, O> requestChannel(RequestChannel<I, O> handler) { requestChannel = handler; return this; }
        public RpcHandlers<I, O> build() { return new RpcHandlers<>(this); }
    }
}
