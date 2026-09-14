package com.byeolnaerim.prp;

import java.util.concurrent.CompletionStage;

public interface SignalAcceptor {
    boolean accepts(ReactiveSession session, SessionSignal signal);
    CompletionStage<Void> handle(ReactiveSession session, SessionSignal signal);
}
