package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.ReactiveSession;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public interface PrpContext {
    String route();
    PrpInteraction interaction();
    PrpWireProtocol wireProtocol();
    String transport();
    PrpRequester requester();
    Optional<ReactiveSession> nativeSession();
    boolean datagramsAvailable();
    CompletionStage<Void> sendDatagram(String route, byte[] data);
    CompletionStage<Void> sendLatestDatagram(String route, byte[] data);
}
