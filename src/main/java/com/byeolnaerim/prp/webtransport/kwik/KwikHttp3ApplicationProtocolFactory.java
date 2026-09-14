package com.byeolnaerim.prp.webtransport.kwik;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.server.ApplicationProtocolConnection;
import tech.kwik.core.server.ApplicationProtocolConnectionFactory;
import tech.kwik.flupke.server.Http3ServerExtensionFactory;
import tech.kwik.flupke.server.HttpRequestHandler;

/** Creates HTTP/3 connections with the transport extensions required by WebTransport draft-16. */
final class KwikHttp3ApplicationProtocolFactory implements ApplicationProtocolConnectionFactory {
    static final String HTTP3_ALPN = "h3";

    private static final long MAX_HEADER_SIZE = 64 * 1024L;
    private static final long MAX_HTTP_DATA_SIZE = 64 * 1024 * 1024L;

    private final KwikWebTransportExtensionFactory webTransportExtensionFactory;
    private final ExecutorService executor;
    private final int maxSessions;
    private final int maxBidiPerSession;
    private final int maxUniPerSession;
    private final int streamBufferBytes;

    KwikHttp3ApplicationProtocolFactory(
        KwikWebTransportExtensionFactory webTransportExtensionFactory,
        ExecutorService executor,
        int maxSessions,
        int maxBidiPerSession,
        int maxUniPerSession,
        long streamBufferBytes
    ) {
        this.webTransportExtensionFactory = webTransportExtensionFactory;
        this.executor = executor;
        this.maxSessions = maxSessions;
        this.maxBidiPerSession = maxBidiPerSession;
        this.maxUniPerSession = maxUniPerSession;
        this.streamBufferBytes = (int) Math.min(Integer.MAX_VALUE, streamBufferBytes);
    }

    @Override
    public ApplicationProtocolConnection createConnection(String protocol, QuicConnection quicConnection) {
        HttpRequestHandler requestHandler = (request, response) -> {
            // WebTransport extended CONNECT is handled by the extension. Plain HTTP/3 is not served by this carrier.
        };
        Map<String, Http3ServerExtensionFactory> extensions = webTransportExtensionFactory.extensionMap();
        KwikHttp3ServerConnection connection = new KwikHttp3ServerConnection(
            quicConnection,
            requestHandler,
            MAX_HEADER_SIZE,
            MAX_HTTP_DATA_SIZE,
            executor,
            extensions
        );
        KwikWebTransportConnectionState state = webTransportExtensionFactory.prepareConnection(connection);
        quicConnection.setConnectionListener(event -> state.close());
        return connection;
    }

    @Override
    public int maxConcurrentPeerInitiatedUnidirectionalStreams() {
        long requested = 3L + (long) maxSessions * maxUniPerSession + 8L;
        return (int) Math.min(Integer.MAX_VALUE, requested);
    }

    @Override
    public long maxTotalPeerInitiatedUnidirectionalStreams() {
        return Long.MAX_VALUE;
    }

    @Override
    public int maxConcurrentPeerInitiatedBidirectionalStreams() {
        long requested = (long) maxSessions * (Math.max(1, maxBidiPerSession) + 1L) + 8L;
        return (int) Math.min(Integer.MAX_VALUE, requested);
    }

    @Override
    public long maxTotalPeerInitiatedBidirectionalStreams() {
        return Long.MAX_VALUE;
    }

    @Override
    public int minUnidirectionalStreamReceiverBufferSize() {
        return Math.max(1024, Math.min(streamBufferBytes, 64 * 1024));
    }

    @Override
    public long maxUnidirectionalStreamReceiverBufferSize() {
        return streamBufferBytes;
    }

    @Override
    public int minBidirectionalStreamReceiverBufferSize() {
        return Math.max(1024, Math.min(streamBufferBytes, 64 * 1024));
    }

    @Override
    public long maxBidirectionalStreamReceiverBufferSize() {
        return streamBufferBytes;
    }

    @Override
    public boolean enableDatagramExtension() {
        return true;
    }

    @Override
    public boolean enableReliableStreamReset() {
        return true;
    }
}
