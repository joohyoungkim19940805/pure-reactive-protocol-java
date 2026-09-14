package com.byeolnaerim.prp.reactor;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.ReactiveStream;
import com.byeolnaerim.prp.StreamMessage;
import java.math.BigInteger;
import java.util.List;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ReactorStream {
    private final ReactiveStream delegate;
    public ReactorStream(ReactiveStream delegate) { this.delegate = java.util.Objects.requireNonNull(delegate); }
    public ReactiveStream delegate() { return delegate; }
    public BigInteger id() { return delegate.id(); }
    public List<ProtocolAttribute> attributes() { return delegate.attributes(); }
    public boolean closed() { return delegate.closed(); }
    public Flux<StreamMessage> receive() { return JdkFlowAdapter.flowPublisherToFlux(delegate); }
    public Mono<Void> request(long count) { return Mono.fromCompletionStage(delegate.request(count)); }
    public Mono<Void> send(byte[] data) { return Mono.fromCompletionStage(delegate.send(data)); }
    public Mono<Void> send(byte[] data, List<ProtocolAttribute> attributes) { return Mono.fromCompletionStage(delegate.send(data, attributes)); }
    public Mono<Void> complete() { return Mono.fromCompletionStage(delegate.complete()); }
    public Mono<Void> cancel(String reason) { return Mono.fromCompletionStage(delegate.cancel(reason)); }
    public Mono<Void> fail(Throwable error) { return Mono.fromCompletionStage(delegate.fail(error)); }
}
