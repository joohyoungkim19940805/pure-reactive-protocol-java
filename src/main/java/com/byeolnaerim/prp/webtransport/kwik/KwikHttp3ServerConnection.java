package com.byeolnaerim.prp.webtransport.kwik;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.QuicStream;
import tech.kwik.flupke.HttpStream;
import tech.kwik.flupke.server.Http3ServerExtensionFactory;
import tech.kwik.flupke.server.HttpRequestHandler;
import tech.kwik.flupke.server.impl.Http3ServerConnectionImpl;
import java.util.Map;

/** HTTP/3 connection that recognizes current WebTransport stream association headers before normal H3 dispatch. */
final class KwikHttp3ServerConnection extends Http3ServerConnectionImpl {
    private volatile Consumer<KwikIncomingWebTransportStream> webTransportStreamHandler;
    private final ExecutorService callbackExecutor;

    KwikHttp3ServerConnection(
        QuicConnection quicConnection,
        HttpRequestHandler requestHandler,
        long maxHeaderSize,
        long maxDataSize,
        ExecutorService executor,
        Map<String, Http3ServerExtensionFactory> extensions
    ) {
        super(quicConnection, requestHandler, maxHeaderSize, maxDataSize, executor, extensions);
        this.callbackExecutor = executor;
    }

    void webTransportStreamHandler(Consumer<KwikIncomingWebTransportStream> handler) {
        this.webTransportStreamHandler = handler;
    }

    void webTransportDatagramHandler(Consumer<byte[]> handler) {
        quicConnection.setDatagramHandler(handler, callbackExecutor);
    }

    boolean canSendWebTransportDatagrams() {
        return quicConnection.canSendDatagram() && quicConnection.canReceiveDatagram();
    }

    int maxWebTransportDatagramPayload(long sessionId) {
        long quarterStreamId = sessionId / 4L;
        return Math.max(0, quicConnection.maxDatagramDataSize() - KwikWebTransportProtocol.varIntLength(quarterStreamId));
    }

    void sendWebTransportDatagram(long sessionId, byte[] payload) {
        if (!canSendWebTransportDatagrams()) throw new IllegalStateException("QUIC datagrams are unavailable on this connection.");
        if (payload == null) throw new NullPointerException("payload");
        long quarterStreamId = sessionId / 4L;
        int maxPayload = maxWebTransportDatagramPayload(sessionId);
        if (payload.length > maxPayload) throw new IllegalArgumentException("WebTransport datagram exceeds " + maxPayload + " bytes.");
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream(KwikWebTransportProtocol.varIntLength(quarterStreamId) + payload.length);
            KwikWebTransportProtocol.writeVarInt(output, quarterStreamId);
            output.write(payload);
            quicConnection.sendDatagram(output.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }


    @Override
    protected void handleBidirectionalStream(QuicStream quicStream) {
        Consumer<KwikIncomingWebTransportStream> handler = webTransportStreamHandler;
        if (handler == null) {
            super.handleBidirectionalStream(quicStream);
            return;
        }

        PushbackInputStream input = new PushbackInputStream(quicStream.getInputStream(), 16);
        try {
            KwikWebTransportProtocol.VarInt signal = KwikWebTransportProtocol.readVarInt(input);
            if (signal.value() != KwikWebTransportProtocol.BIDIRECTIONAL_STREAM_SIGNAL) {
                input.unread(signal.encoded());
                super.handleBidirectionalStream(withInput(quicStream, input));
                return;
            }
            KwikWebTransportProtocol.VarInt sessionId = KwikWebTransportProtocol.readVarInt(input);
            if (!KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(sessionId.value())) {
                closeConnection(KwikWebTransportProtocol.H3_ID_ERROR, "Invalid WebTransport session ID");
                return;
            }
            if (!peerNegotiatedSupportedWebTransport()) {
                closeConnection(KwikWebTransportProtocol.WT_REQUIREMENTS_NOT_MET, "WebTransport requirements not met");
                return;
            }
            handler.accept(new KwikIncomingWebTransportStream(
                sessionId.value(),
                quicStream,
                input,
                true
            ));
        } catch (Throwable error) {
            rejectMalformed(quicStream, error);
        }
    }

    @Override
    protected void handleUnidirectionalStream(QuicStream quicStream) {
        Consumer<KwikIncomingWebTransportStream> handler = webTransportStreamHandler;
        if (handler == null) {
            super.handleUnidirectionalStream(quicStream);
            return;
        }

        PushbackInputStream input = new PushbackInputStream(quicStream.getInputStream(), 16);
        try {
            KwikWebTransportProtocol.VarInt streamType = KwikWebTransportProtocol.readVarInt(input);
            if (streamType.value() != KwikWebTransportProtocol.UNIDIRECTIONAL_STREAM_TYPE) {
                input.unread(streamType.encoded());
                super.handleUnidirectionalStream(withInput(quicStream, input));
                return;
            }
            KwikWebTransportProtocol.VarInt sessionId = KwikWebTransportProtocol.readVarInt(input);
            if (!KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(sessionId.value())) {
                closeConnection(KwikWebTransportProtocol.H3_ID_ERROR, "Invalid WebTransport session ID");
                return;
            }
            if (!peerNegotiatedSupportedWebTransport()) {
                closeConnection(KwikWebTransportProtocol.WT_REQUIREMENTS_NOT_MET, "WebTransport requirements not met");
                return;
            }
            handler.accept(new KwikIncomingWebTransportStream(
                sessionId.value(),
                quicStream,
                input,
                false
            ));
        } catch (Throwable error) {
            rejectMalformed(quicStream, error);
        }
    }

    boolean peerSupportsRequiredTransportParameters() {
        return quicConnection.isDatagramExtensionEnabled() && quicConnection.canUseReliableStreamReset();
    }

    boolean peerSupportsLegacyTransportParameters() {
        return quicConnection.isDatagramExtensionEnabled();
    }

    void closeWebTransportRequirementsNotMet() {
        closeConnection(KwikWebTransportProtocol.WT_REQUIREMENTS_NOT_MET, "WebTransport requirements not met");
    }

    private boolean peerNegotiatedSupportedWebTransport() {
        if (getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_H3_DATAGRAM).orElse(0L) != 1L) {
            return false;
        }

        if (getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_WT_ENABLED).orElse(0L) == 1L) {
            return peerSupportsRequiredTransportParameters();
        }

        return (
            getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07).orElse(0L) > 0L
                || getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02).orElse(0L) == 1L
        ) && peerSupportsLegacyTransportParameters();
    }

    private void closeConnection(long errorCode, String reason) {
        try {
            quicConnection.close(errorCode, reason);
        } catch (Throwable ignored) {
        }
    }

    private void rejectMalformed(QuicStream stream, Throwable ignored) {
        closeConnection(KwikWebTransportProtocol.H3_FRAME_ERROR, "Malformed WebTransport stream header");
    }

    private static QuicStream withInput(QuicStream delegate, InputStream input) {
        return new QuicStream() {
            @Override public InputStream getInputStream() { return input; }
            @Override public java.io.OutputStream getOutputStream() { return delegate.getOutputStream(); }
            @Override public int getStreamId() { return delegate.getStreamId(); }
            @Override public boolean isUnidirectional() { return delegate.isUnidirectional(); }
            @Override public boolean isClientInitiatedBidirectional() { return delegate.isClientInitiatedBidirectional(); }
            @Override public boolean isServerInitiatedBidirectional() { return delegate.isServerInitiatedBidirectional(); }
            @Override public void abortReading(long errorCode) { delegate.abortReading(errorCode); }
            @Override public void resetStream(long errorCode) { delegate.resetStream(errorCode); }
            @Override public void resetStreamAt(long errorCode, long reliableSize) { delegate.resetStreamAt(errorCode, reliableSize); }
            @Override public void resetStreamAt(long errorCode) { delegate.resetStreamAt(errorCode); }
        };
    }
}
