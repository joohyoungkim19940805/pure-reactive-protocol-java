package com.byeolnaerim.prp.spring;

import io.rsocket.SocketAcceptor;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;

/**
 * Spring Boot WebSocket RSocket bootstrap bridge.
 *
 * <p>PRP compatibility routes and regular Spring {@code @MessageMapping} routes share the
 * same RSocket connection. Exact {@code @PrpRoute} matches are dispatched through
 * {@link PrpRouteRegistry}; every other RSocket request falls back to Spring's native
 * {@link RSocketMessageHandler} routing.</p>
 */
public final class PrpRSocketBootstrapHandler extends RSocketMessageHandler {
    private final PrpRSocketRegistryAdapter adapter;
    private final String transport;

    public PrpRSocketBootstrapHandler(
        PrpRSocketRegistryAdapter adapter,
        String transport,
        RSocketStrategies strategies
    ) {
        this.adapter = java.util.Objects.requireNonNull(adapter);
        this.transport = java.util.Objects.requireNonNull(transport);
        setRSocketStrategies(java.util.Objects.requireNonNull(strategies));
    }

    @Override
    public SocketAcceptor responder() {
        return adapter.acceptor(transport, super.responder());
    }
}
