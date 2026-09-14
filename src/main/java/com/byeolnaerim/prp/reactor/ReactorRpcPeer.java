package com.byeolnaerim.prp.reactor;

import com.byeolnaerim.prp.profile.rpc.RpcPeer;
import java.util.concurrent.Flow;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ReactorRpcPeer {
    private final RpcPeer delegate;
    public ReactorRpcPeer(RpcPeer delegate) { this.delegate = java.util.Objects.requireNonNull(delegate); }
    public RpcPeer delegate() { return delegate; }
    public <I, O> Mono<O> requestResponse(String target, I input, Class<O> responseType) {
        return Mono.fromCompletionStage(delegate.requestResponse(target, input, responseType));
    }
    public <I> Mono<Void> fireAndForget(String target, I input) {
        return Mono.fromCompletionStage(delegate.fireAndForget(target, input));
    }
    public <I, O> Flux<O> requestStream(String target, I input, Class<O> responseType) {
        return Mono.fromCompletionStage(delegate.requestStream(target, input, responseType)).flatMapMany(JdkFlowAdapter::flowPublisherToFlux);
    }
    public <I, O> Flux<O> requestChannel(String target, Flux<I> input, Class<O> responseType) {
        Flow.Publisher<I> flowInput = JdkFlowAdapter.publisherToFlowPublisher(input);
        return Mono.fromCompletionStage(delegate.requestChannel(target, flowInput, responseType)).flatMapMany(JdkFlowAdapter::flowPublisherToFlux);
    }
}
