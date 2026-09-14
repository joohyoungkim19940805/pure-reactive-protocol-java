package com.byeolnaerim.prp;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

public interface ReactiveSession extends Flow.Publisher<ReactiveStream> {
    SessionState state();
    String sessionId();
    Flow.Publisher<SessionSignal> signals();
    Flow.Publisher<SessionDatagram> datagrams();
    /** Maximum application payload for one native PRP datagram; 0 when unavailable. */
    int maxDatagramBytes();
    boolean supports(String capabilityId);
    AutoCloseable onStateChange(Consumer<SessionState> listener);
    CompletionStage<ReactiveStream> open(List<ProtocolAttribute> attributes);
    default CompletionStage<ReactiveStream> open() { return open(List.of()); }
    CompletionStage<Void> signal(List<ProtocolAttribute> attributes, byte[] payload);
    default CompletionStage<Void> signal(List<ProtocolAttribute> attributes) { return signal(attributes, new byte[0]); }
    /** Best-effort/unordered send. Completion means accepted by the carrier, not delivered or acknowledged. */
    CompletionStage<Void> sendDatagram(byte[] data);
    CompletionStage<Void> close(String reason);
    default CompletionStage<Void> close() { return close(null); }
}
