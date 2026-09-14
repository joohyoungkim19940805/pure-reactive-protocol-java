package com.byeolnaerim.prp;

import java.util.concurrent.CompletionStage;

public interface StreamAcceptor {
    boolean accepts(ReactiveSession session, ReactiveStream stream);
    CompletionStage<Void> handle(ReactiveSession session, ReactiveStream stream);
}
