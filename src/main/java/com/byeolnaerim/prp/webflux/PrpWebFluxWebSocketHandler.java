package com.byeolnaerim.prp.webflux;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.core.PrpRuntime;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Mono;

public final class PrpWebFluxWebSocketHandler implements WebSocketHandler {
    private final PrpRuntime runtime;
    private final Function<ReactiveSession, CompletionStage<Void>> onSession;

    public PrpWebFluxWebSocketHandler(PrpRuntime runtime) {
        this(runtime, session -> java.util.concurrent.CompletableFuture.completedFuture(null));
    }

    public PrpWebFluxWebSocketHandler(PrpRuntime runtime, Function<ReactiveSession, CompletionStage<Void>> onSession) {
        this.runtime = java.util.Objects.requireNonNull(runtime);
        this.onSession = java.util.Objects.requireNonNull(onSession);
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        WebFluxWebSocketTransport transport = new WebFluxWebSocketTransport(webSocketSession);
        Mono<Void> accepted = Mono.fromCompletionStage(runtime.accept(transport))
            .flatMap(session -> Mono.fromCompletionStage(onSession.apply(session)))
            .onErrorResume(error -> webSocketSession.close())
            .then();
        Mono<Void> outbound = transport.outbound();
        Mono<Void> closed = Mono.fromCompletionStage(transport.closed()).onErrorResume(error -> Mono.empty()).then();
        return Mono.whenDelayError(accepted, outbound, closed).then();
    }
}
