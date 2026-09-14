package com.byeolnaerim.prp.reactor;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionSignal;
import com.byeolnaerim.prp.SessionDatagram;
import com.byeolnaerim.prp.SessionState;
import java.util.List;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ReactorSession {
    private final ReactiveSession delegate;
    public ReactorSession(ReactiveSession delegate) { this.delegate = java.util.Objects.requireNonNull(delegate); }
    public ReactiveSession delegate() { return delegate; }
    public SessionState state() { return delegate.state(); }
    public String sessionId() { return delegate.sessionId(); }
    public boolean supports(String capabilityId) { return delegate.supports(capabilityId); }
    public Flux<ReactorStream> incomingStreams() { return JdkFlowAdapter.flowPublisherToFlux(delegate).map(ReactorStream::new); }
    public Flux<SessionSignal> signals() { return JdkFlowAdapter.flowPublisherToFlux(delegate.signals()); }
    public Flux<SessionDatagram> datagrams() { return JdkFlowAdapter.flowPublisherToFlux(delegate.datagrams()); }
    public int maxDatagramBytes() { return delegate.maxDatagramBytes(); }
    public Mono<Void> sendDatagram(byte[] data) { return Mono.fromCompletionStage(delegate.sendDatagram(data)); }
    public Mono<ReactorStream> open() { return Mono.fromCompletionStage(delegate.open()).map(ReactorStream::new); }
    public Mono<ReactorStream> open(List<ProtocolAttribute> attributes) { return Mono.fromCompletionStage(delegate.open(attributes)).map(ReactorStream::new); }
    public Mono<Void> signal(List<ProtocolAttribute> attributes, byte[] payload) { return Mono.fromCompletionStage(delegate.signal(attributes, payload)); }
    public Mono<Void> close(String reason) { return Mono.fromCompletionStage(delegate.close(reason)); }
}
