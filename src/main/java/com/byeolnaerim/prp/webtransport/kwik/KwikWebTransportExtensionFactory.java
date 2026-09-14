package com.byeolnaerim.prp.webtransport.kwik;

import com.byeolnaerim.prp.webtransport.WebTransportHandler;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import tech.kwik.flupke.server.Http3ServerConnection;
import tech.kwik.flupke.server.Http3ServerExtension;
import tech.kwik.flupke.server.Http3ServerExtensionFactory;

final class KwikWebTransportExtensionFactory implements Http3ServerExtensionFactory, AutoCloseable {
    private final Map<String, WebTransportHandler> routes;
    private final Set<String> allowedOrigins;
    private final ExecutorService executor;
    private final int maxSessions;
    private final int maxBidiPerSession;
    private final int maxUniPerSession;
    private final int maxBufferedStreams;
    private final Map<KwikHttp3ServerConnection, KwikWebTransportConnectionState> states = new ConcurrentHashMap<>();

    KwikWebTransportExtensionFactory(
        Map<String, WebTransportHandler> routes,
        Set<String> allowedOrigins,
        ExecutorService executor,
        int maxSessions,
        int maxBidiPerSession,
        int maxUniPerSession,
        int maxBufferedStreams
    ) {
        this.routes = Map.copyOf(routes);
        this.allowedOrigins = Set.copyOf(allowedOrigins);
        this.executor = Objects.requireNonNull(executor, "executor");
        this.maxSessions = maxSessions;
        this.maxBidiPerSession = maxBidiPerSession;
        this.maxUniPerSession = maxUniPerSession;
        this.maxBufferedStreams = maxBufferedStreams;
    }

    Map<String, Http3ServerExtensionFactory> extensionMap() {
        return Map.of(
            KwikWebTransportProtocol.CURRENT_PROTOCOL, this,
            KwikWebTransportProtocol.LEGACY_DRAFT07_PROTOCOL, this
        );
    }

    KwikWebTransportConnectionState prepareConnection(KwikHttp3ServerConnection connection) {
        return state(connection);
    }

    private KwikWebTransportConnectionState state(KwikHttp3ServerConnection connection) {
        return states.computeIfAbsent(connection, key -> {
            KwikWebTransportConnectionState state = new KwikWebTransportConnectionState(
                key,
                routes,
                allowedOrigins,
                executor,
                maxSessions,
                maxBidiPerSession,
                maxUniPerSession,
                maxBufferedStreams,
                () -> states.remove(key)
            );
            key.webTransportStreamHandler(state::onIncomingStream);
            return state;
        });
    }

    @Override
    public Http3ServerExtension createExtension(Http3ServerConnection connection) {
        if (!(connection instanceof KwikHttp3ServerConnection kwikConnection)) {
            throw new IllegalStateException("WebTransport requires KwikHttp3ServerConnection.");
        }
        return state(kwikConnection);
    }

    @Override
    public Map<Long, Long> getExtensionSettings() {
        // Keep current draft-16 and advertise the legacy variants still used by browsers.
        // Chromium currently enables draft-02 by default; draft-07 remains feature-gated.
        return Map.of(
            KwikWebTransportProtocol.SETTINGS_WT_ENABLED, 1L,
            KwikWebTransportProtocol.SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07, (long) maxSessions,
            KwikWebTransportProtocol.SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02, 1L,
            KwikWebTransportProtocol.SETTINGS_H3_DATAGRAM, 1L
        );
    }

    @Override
    public void close() {
        for (KwikWebTransportConnectionState state : states.values()) state.close();
        states.clear();
    }
}
