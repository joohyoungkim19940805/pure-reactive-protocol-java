package com.byeolnaerim.prp.webtransport;

import com.byeolnaerim.prp.webtransport.kwik.KwikWebTransportServer;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collection;

/**
 * Runtime-neutral lifecycle contract for a server-side WebTransport carrier.
 *
 * <p>The default implementation is 100% Java and uses Kwik for QUIC plus Flupke for HTTP/3.
 * Applications do not need to depend on implementation packages.</p>
 */
public interface WebTransportServer extends AutoCloseable {
    /** Creates a builder for the default Java WebTransport server implementation. */
    static Builder builder() {
        return KwikWebTransportServer.builder();
    }

    /** Starts listening. Calling this while already running is a no-op. */
    void start();

    boolean isRunning();

    String host();

    /** Actual bound port after startup, configured port before startup. */
    int port();

    /** Stops listening and closes active sessions. */
    @Override
    void close();

    interface Builder {
        Builder bind(String host, int port);

        /** Configures a private-key entry used for QUIC TLS. */
        Builder tls(KeyStore keyStore, String certificateAlias, char[] privateKeyPassword);

        /** Empty collection or "*" allows every origin. Otherwise matching is exact. */
        Builder allowedOrigins(Collection<String> origins);

        /** Registers an exact WebTransport request path such as /prp/native. */
        Builder route(String path, WebTransportHandler handler);

        Builder idleTimeout(Duration timeout);

        Builder maxConnectionBufferBytes(long bytes);

        Builder maxStreamBufferBytes(long bytes);

        /**
         * Maximum sessions accepted on one HTTP/3 connection. The bundled stream-only implementation
         * currently requires 1 because draft-16 requires session-level WebTransport flow control for pooling.
         */
        Builder maxSessionsPerConnection(int count);

        Builder maxBidirectionalStreamsPerSession(int count);

        Builder maxUnidirectionalStreamsPerSession(int count);

        /** Maximum number of early data streams retained before their CONNECT session is known. */
        Builder maxBufferedStreamsPerConnection(int count);

        WebTransportServer build();
    }
}
