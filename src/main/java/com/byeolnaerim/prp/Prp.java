package com.byeolnaerim.prp;

import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.transport.ReactiveTransport;
import java.util.concurrent.CompletionStage;

public final class Prp {
    private Prp() {}

    public static CompletionStage<ReactiveSession> connect(ReactiveTransport transport) {
        return PrpRuntime.defaults().connect(transport);
    }

    public static CompletionStage<ReactiveSession> accept(ReactiveTransport transport) {
        return PrpRuntime.defaults().accept(transport);
    }
}
