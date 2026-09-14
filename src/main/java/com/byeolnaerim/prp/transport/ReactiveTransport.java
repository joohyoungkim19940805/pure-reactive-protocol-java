package com.byeolnaerim.prp.transport;

import java.util.concurrent.CompletionStage;

public interface ReactiveTransport {
    String id();
    CompletionStage<TransportConnection> connect();
}
