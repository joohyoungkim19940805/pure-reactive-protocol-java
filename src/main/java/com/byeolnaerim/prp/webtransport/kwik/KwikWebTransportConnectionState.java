package com.byeolnaerim.prp.webtransport.kwik;

import com.byeolnaerim.prp.webtransport.WebTransportHandler;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpHeaders;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import tech.kwik.flupke.HttpStream;
import tech.kwik.flupke.server.Http3ServerExtension;

/** Per-HTTP/3-connection WebTransport state. */
final class KwikWebTransportConnectionState implements Http3ServerExtension, AutoCloseable {
    private static final System.Logger LOGGER = System.getLogger(KwikWebTransportConnectionState.class.getName());
    private static final long REJECTED_STREAM_ERROR = 1L;

    private final KwikHttp3ServerConnection connection;
    private final Map<String, WebTransportHandler> routes;
    private final Set<String> allowedOrigins;
    private final ExecutorService executor;
    private final int maxSessions;
    private final int maxBidiPerSession;
    private final int maxUniPerSession;
    private final int maxBufferedStreams;
    private final int maxRememberedClosedSessions;
    private final Runnable onConnectionDisposed;
    private final Object stateLock = new Object();
    private final Map<Long, KwikWebTransportSession> sessions = new LinkedHashMap<>();
    private final LinkedHashMap<Long, Boolean> closedSessions = new LinkedHashMap<>();
    private final Map<Long, List<KwikIncomingWebTransportStream>> earlyStreams = new LinkedHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private int activeSessionSlots;
    private int earlyStreamCount;

    KwikWebTransportConnectionState(
        KwikHttp3ServerConnection connection,
        Map<String, WebTransportHandler> routes,
        Set<String> allowedOrigins,
        ExecutorService executor,
        int maxSessions,
        int maxBidiPerSession,
        int maxUniPerSession,
        int maxBufferedStreams,
        Runnable onConnectionDisposed
    ) {
        this.connection = Objects.requireNonNull(connection, "connection");
        this.routes = routes;
        this.allowedOrigins = allowedOrigins;
        this.executor = executor;
        this.maxSessions = maxSessions;
        this.maxBidiPerSession = maxBidiPerSession;
        this.maxUniPerSession = maxUniPerSession;
        this.maxBufferedStreams = maxBufferedStreams;
        this.maxRememberedClosedSessions = Math.max(64, maxSessions * 8 + maxBufferedStreams * 2);
        this.onConnectionDisposed = onConnectionDisposed;
        this.connection.webTransportDatagramHandler(this::onIncomingDatagram);
    }

    @Override
    public void handleExtendedConnect(
        HttpHeaders headers,
        String protocol,
        String authority,
        String pathAndQuery,
        IntConsumer statusCallback,
        HttpStream requestResponseStream
    ) {
        if (closed.get()) {
            statusCallback.accept(503);
            return;
        }

        if (!KwikWebTransportProtocol.CURRENT_PROTOCOL.equals(protocol)
            && !KwikWebTransportProtocol.LEGACY_DRAFT07_PROTOCOL.equals(protocol)) {
            statusCallback.accept(501);
            return;
        }

        String path = normalizePath(pathAndQuery);
        WebTransportHandler handler = routes.get(path);
        if (handler == null) {
            // WebTransport draft-16 recommends 405 for a resource that does not support WebTransport.
            statusCallback.accept(405);
            return;
        }

        String origin = headers.firstValue("origin").orElse("");
        if (!originAllowed(origin)) {
            statusCallback.accept(403);
            return;
        }

        if (!peerSupportsRequiredSettings(protocol)) {
            rejectEarly(requestResponseStream.getStreamId(), KwikWebTransportProtocol.WT_SESSION_GONE);
            statusCallback.accept(400);
            connection.closeWebTransportRequirementsNotMet();
            return;
        }

        long sessionId = requestResponseStream.getStreamId();
        if (!reserveSessionSlot()) {
            statusCallback.accept(429);
            return;
        }

        KwikWebTransportSession session = new KwikWebTransportSession(
            sessionId,
            path,
            origin,
            authority == null ? "" : authority,
            headers.map(),
            handler,
            requestResponseStream,
            connection,
            executor,
            maxBidiPerSession,
            maxUniPerSession,
            () -> removeSession(sessionId)
        );

        try {
            statusCallback.accept(200);
        } catch (Throwable error) {
            releaseReservedSessionSlot();
            session.abortBeforeEstablished();
            throw error;
        }

        List<KwikIncomingWebTransportStream> buffered;
        synchronized (stateLock) {
            if (closed.get()) {
                activeSessionSlots--;
                buffered = removeEarlyStreamsLocked(sessionId);
            } else {
                sessions.put(sessionId, session);
                buffered = removeEarlyStreamsLocked(sessionId);
            }
        }

        if (closed.get()) {
            rejectAll(buffered, KwikWebTransportProtocol.WT_SESSION_GONE);
            session.abortBeforeEstablished();
            return;
        }

        acceptBuffered(session, buffered);

        // User callbacks and CONNECT-stream monitoring are deliberately independent. A blocking callback
        // must never prevent the server from observing that the session has been closed by the peer.
        executor.execute(() -> {
            try {
                if (session.isOpen()) handler.onSessionReady(session);
            } catch (Throwable error) {
                LOGGER.log(System.Logger.Level.DEBUG, "WebTransport session callback failed", error);
                session.close();
            }
        });
        executor.execute(session::monitorConnectStream);
    }

    private void onIncomingDatagram(byte[] raw) {
        if (raw == null || raw.length == 0 || closed.get()) return;
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(raw);
            long quarterStreamId = KwikWebTransportProtocol.readVarInt(input).value();
            if (quarterStreamId > Long.MAX_VALUE / 4L) return;
            long sessionId = quarterStreamId * 4L;
            KwikWebTransportSession session;
            synchronized (stateLock) { session = sessions.get(sessionId); }
            if (session == null || !session.isOpen()) return;
            session.acceptDatagram(input.readAllBytes());
        } catch (Throwable ignored) {
            // HTTP/3 datagrams are best-effort. Malformed or stale session datagrams are dropped.
        }
    }

    void onIncomingStream(KwikIncomingWebTransportStream incoming) {
        KwikWebTransportSession session = null;
        long rejectCode = -1;

        synchronized (stateLock) {
            if (closed.get()) {
                rejectCode = KwikWebTransportProtocol.WT_SESSION_GONE;
            } else {
                session = sessions.get(incoming.sessionId());
                if (session == null || !session.isOpen()) {
                    session = null;
                    if (closedSessions.containsKey(incoming.sessionId())) {
                        rejectCode = KwikWebTransportProtocol.WT_SESSION_GONE;
                    } else if (earlyStreamCount >= maxBufferedStreams) {
                        rejectCode = KwikWebTransportProtocol.WT_BUFFERED_STREAM_REJECTED;
                    } else {
                        earlyStreams.computeIfAbsent(incoming.sessionId(), ignored -> new ArrayList<>()).add(incoming);
                        earlyStreamCount++;
                    }
                }
            }
        }

        if (session != null) {
            session.accept(incoming);
        } else if (rejectCode >= 0) {
            reject(incoming, rejectCode);
        }
    }

    private boolean peerSupportsRequiredSettings(String protocol) {
        // getPeerSettingsParameter waits for the peer SETTINGS frame in Flupke, which also satisfies
        // the draft-version negotiation requirement to wait for SETTINGS before processing WebTransport.
        long datagram = connection.getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_H3_DATAGRAM).orElse(0L);
        if (datagram != 1L) return false;

        if (KwikWebTransportProtocol.CURRENT_PROTOCOL.equals(protocol)) {
            long webTransport = connection.getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_WT_ENABLED).orElse(0L);
            return webTransport == 1L && connection.peerSupportsRequiredTransportParameters();
        }

        long draft07 = connection
            .getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07)
            .orElse(0L);
        long draft02 = connection
            .getPeerSettingsParameter(KwikWebTransportProtocol.SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02)
            .orElse(0L);
        return (draft07 > 0L || draft02 == 1L) && connection.peerSupportsLegacyTransportParameters();
    }

    private boolean reserveSessionSlot() {
        synchronized (stateLock) {
            if (closed.get() || activeSessionSlots >= maxSessions) return false;
            activeSessionSlots++;
            return true;
        }
    }

    private void releaseReservedSessionSlot() {
        synchronized (stateLock) {
            if (activeSessionSlots > 0) activeSessionSlots--;
        }
    }

    private boolean originAllowed(String origin) {
        if (allowedOrigins.isEmpty() || allowedOrigins.contains("*")) return true;
        return !origin.isBlank() && allowedOrigins.contains(origin);
    }

    private void acceptBuffered(KwikWebTransportSession session, List<KwikIncomingWebTransportStream> buffered) {
        if (buffered == null) return;
        for (KwikIncomingWebTransportStream incoming : buffered) {
            if (session.isOpen()) session.accept(incoming);
            else reject(incoming, KwikWebTransportProtocol.WT_SESSION_GONE);
        }
    }

    private void removeSession(long sessionId) {
        List<KwikIncomingWebTransportStream> buffered;
        synchronized (stateLock) {
            KwikWebTransportSession removed = sessions.remove(sessionId);
            if (removed == null) return;
            if (activeSessionSlots > 0) activeSessionSlots--;
            rememberClosedSessionLocked(sessionId);
            buffered = removeEarlyStreamsLocked(sessionId);
        }
        rejectAll(buffered, KwikWebTransportProtocol.WT_SESSION_GONE);
    }

    private void rememberClosedSessionLocked(long sessionId) {
        closedSessions.put(sessionId, Boolean.TRUE);
        while (closedSessions.size() > maxRememberedClosedSessions) {
            Long oldest = closedSessions.keySet().iterator().next();
            closedSessions.remove(oldest);
        }
    }

    private List<KwikIncomingWebTransportStream> removeEarlyStreamsLocked(long sessionId) {
        List<KwikIncomingWebTransportStream> buffered = earlyStreams.remove(sessionId);
        if (buffered != null) earlyStreamCount -= buffered.size();
        return buffered;
    }

    private void rejectEarly(long sessionId, long wireError) {
        List<KwikIncomingWebTransportStream> buffered;
        synchronized (stateLock) {
            buffered = removeEarlyStreamsLocked(sessionId);
        }
        rejectAll(buffered, wireError);
    }

    private static String normalizePath(String pathAndQuery) {
        if (pathAndQuery == null || pathAndQuery.isBlank()) return "/";
        try {
            URI uri = URI.create(pathAndQuery);
            String path = uri.getPath();
            return path == null || path.isBlank() ? "/" : path;
        } catch (IllegalArgumentException ignored) {
            int query = pathAndQuery.indexOf('?');
            String path = query >= 0 ? pathAndQuery.substring(0, query) : pathAndQuery;
            return path.isBlank() ? "/" : path;
        }
    }

    private static void rejectAll(List<KwikIncomingWebTransportStream> streams, long wireError) {
        if (streams == null) return;
        streams.forEach(incoming -> reject(incoming, wireError));
    }

    private static void reject(KwikIncomingWebTransportStream incoming) {
        reject(incoming, KwikWebTransportProtocol.toHttp3Error(REJECTED_STREAM_ERROR));
    }

    private static void reject(KwikIncomingWebTransportStream incoming, long wireError) {
        try {
            incoming.quicStream().abortReading(wireError);
        } catch (Throwable ignored) {
        }
        if (incoming.bidirectional()) {
            try {
                incoming.quicStream().resetStream(wireError);
            } catch (Throwable ignored) {
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;

        List<KwikWebTransportSession> active;
        List<KwikIncomingWebTransportStream> buffered = new ArrayList<>();
        synchronized (stateLock) {
            active = new ArrayList<>(sessions.values());
            earlyStreams.values().forEach(buffered::addAll);
            earlyStreams.clear();
            earlyStreamCount = 0;
        }

        active.forEach(KwikWebTransportSession::close);
        rejectAll(buffered, KwikWebTransportProtocol.WT_SESSION_GONE);

        synchronized (stateLock) {
            sessions.clear();
            activeSessionSlots = 0;
            closedSessions.clear();
        }
        onConnectionDisposed.run();
    }
}
