package com.byeolnaerim.prp.spring;

import io.rsocket.SocketAcceptor;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;

/** Spring Boot WebSocket RSocket bootstrap bridge. Application dispatch stays in PrpRouteRegistry. */
public final class PrpRSocketBootstrapHandler extends RSocketMessageHandler {
    private final SocketAcceptor acceptor;

    public PrpRSocketBootstrapHandler(PrpRSocketRegistryAdapter adapter, String transport) {
        this.acceptor = java.util.Objects.requireNonNull(adapter).acceptor(transport);
        setHandlerPredicate(type -> false);
    }

    @Override
    public SocketAcceptor responder() {
        return acceptor;
    }
}
