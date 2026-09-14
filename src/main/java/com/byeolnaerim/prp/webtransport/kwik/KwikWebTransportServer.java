package com.byeolnaerim.prp.webtransport.kwik;

import com.byeolnaerim.prp.webtransport.WebTransportHandler;
import com.byeolnaerim.prp.webtransport.WebTransportServer;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import tech.kwik.core.log.NullLogger;
import tech.kwik.core.server.ServerConnectionConfig;
import tech.kwik.core.server.ServerConnector;

/** Default PRP WebTransport server backed by the pure-Java Kwik/Flupke stack. */
public final class KwikWebTransportServer implements WebTransportServer {
    private static final System.Logger LOGGER = System.getLogger(KwikWebTransportServer.class.getName());

    private final String host;
    private final int configuredPort;
    private final KeyStore keyStore;
    private final String certificateAlias;
    private final char[] privateKeyPassword;
    private final Set<String> allowedOrigins;
    private final Map<String, WebTransportHandler> routes;
    private final Duration idleTimeout;
    private final long maxConnectionBufferBytes;
    private final long maxStreamBufferBytes;
    private final int maxSessionsPerConnection;
    private final int maxBidiPerSession;
    private final int maxUniPerSession;
    private final int maxBufferedStreams;
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile int boundPort;
    private volatile DatagramSocket socket;
    private volatile ServerConnector connector;
    private volatile ExecutorService executor;
    private volatile KwikWebTransportExtensionFactory extensionFactory;

    private KwikWebTransportServer(BuilderImpl builder) {
        host = builder.host;
        configuredPort = builder.port;
        boundPort = builder.port;
        keyStore = builder.keyStore;
        certificateAlias = builder.certificateAlias;
        privateKeyPassword = builder.privateKeyPassword.clone();
        allowedOrigins = Set.copyOf(builder.allowedOrigins);
        routes = Map.copyOf(builder.routes);
        idleTimeout = builder.idleTimeout;
        maxConnectionBufferBytes = builder.maxConnectionBufferBytes;
        maxStreamBufferBytes = builder.maxStreamBufferBytes;
        maxSessionsPerConnection = builder.maxSessionsPerConnection;
        maxBidiPerSession = builder.maxBidiPerSession;
        maxUniPerSession = builder.maxUniPerSession;
        maxBufferedStreams = builder.maxBufferedStreams;
    }

    public static WebTransportServer.Builder builder() {
        return new BuilderImpl();
    }

    @Override
    public synchronized void start() {
        if (running.get()) return;
        validateTls();

        DatagramSocket createdSocket = null;
        ExecutorService createdExecutor = null;
        KwikWebTransportExtensionFactory createdExtensionFactory = null;
        ServerConnector createdConnector = null;
        try {
            createdSocket = new DatagramSocket(null);
            createdSocket.setReuseAddress(true);
            createdSocket.bind(new InetSocketAddress(host, configuredPort));

            createdExecutor = Executors.newVirtualThreadPerTaskExecutor();
            createdExtensionFactory = new KwikWebTransportExtensionFactory(
                routes,
                allowedOrigins,
                createdExecutor,
                maxSessionsPerConnection,
                maxBidiPerSession,
                maxUniPerSession,
                maxBufferedStreams
            );
            KwikHttp3ApplicationProtocolFactory applicationFactory = new KwikHttp3ApplicationProtocolFactory(
                createdExtensionFactory,
                createdExecutor,
                maxSessionsPerConnection,
                maxBidiPerSession,
                maxUniPerSession,
                maxStreamBufferBytes
            );

            int maxOpenBidi = boundedStreamCount((long) maxSessionsPerConnection * (maxBidiPerSession + 1L) + 8L);
            int maxOpenUni = boundedStreamCount(3L + (long) maxSessionsPerConnection * maxUniPerSession + 8L);
            ServerConnectionConfig connectionConfig = ServerConnectionConfig.builder()
                .maxIdleTimeout((int) Math.min(Integer.MAX_VALUE, idleTimeout.toMillis()))
                .maxConnectionBufferSize(maxConnectionBufferBytes)
                .maxBidirectionalStreamBufferSize(maxStreamBufferBytes)
                .maxUnidirectionalStreamBufferSize(maxStreamBufferBytes)
                .maxOpenPeerInitiatedBidirectionalStreams(maxOpenBidi)
                .maxOpenPeerInitiatedUnidirectionalStreams(maxOpenUni)
                .maxTotalPeerInitiatedBidirectionalStreams(Long.MAX_VALUE)
                .maxTotalPeerInitiatedUnidirectionalStreams(Long.MAX_VALUE)
                .build();

            // Kwik 0.11 does not provide a default Logger; ServerConnectorImpl requires one.
            createdConnector = ServerConnector.builder()
                .withSocket(createdSocket)
                .withKeyStore(keyStore, certificateAlias, privateKeyPassword)
                .withConfiguration(connectionConfig)
                .withLogger(new NullLogger())
                .build();
            createdConnector.registerApplicationProtocol(KwikHttp3ApplicationProtocolFactory.HTTP3_ALPN, applicationFactory);
            createdConnector.start();

            socket = createdSocket;
            executor = createdExecutor;
            extensionFactory = createdExtensionFactory;
            connector = createdConnector;
            boundPort = createdSocket.getLocalPort();
            running.set(true);
            LOGGER.log(System.Logger.Level.INFO, "WebTransport server listening on https://{0}:{1} (QUIC/HTTP3, pure Java)", host, boundPort);
        } catch (Throwable error) {
            closeQuietly(createdConnector);
            closeQuietly(createdExtensionFactory);
            if (createdSocket != null) createdSocket.close();
            if (createdExecutor != null) createdExecutor.shutdownNow();
            throw new IllegalStateException("Failed to start WebTransport server on " + host + ":" + configuredPort, error);
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public String host() {
        return host;
    }

    @Override
    public int port() {
        return boundPort;
    }

    @Override
    public synchronized void close() {
        if (!running.getAndSet(false) && connector == null && socket == null) return;

        ServerConnector currentConnector = connector;
        connector = null;
        closeQuietly(currentConnector);

        KwikWebTransportExtensionFactory currentExtensionFactory = extensionFactory;
        extensionFactory = null;
        closeQuietly(currentExtensionFactory);

        DatagramSocket currentSocket = socket;
        socket = null;
        if (currentSocket != null) currentSocket.close();

        ExecutorService currentExecutor = executor;
        executor = null;
        if (currentExecutor != null) currentExecutor.shutdownNow();
    }

    private void validateTls() {
        if (keyStore == null) throw new IllegalStateException("WebTransport TLS key store is required.");
        try {
            if (!keyStore.isKeyEntry(certificateAlias)) {
                throw new IllegalStateException("WebTransport TLS alias is not a private-key entry: " + certificateAlias);
            }
        } catch (java.security.KeyStoreException error) {
            throw new IllegalStateException("Unable to inspect WebTransport TLS key store.", error);
        }
    }

    private static int boundedStreamCount(long value) {
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, value));
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    public static final class BuilderImpl implements WebTransportServer.Builder {
        private String host = "0.0.0.0";
        private int port = 8443;
        private KeyStore keyStore;
        private String certificateAlias = "webtransport";
        private char[] privateKeyPassword = new char[0];
        private final Set<String> allowedOrigins = new LinkedHashSet<>();
        private final Map<String, WebTransportHandler> routes = new LinkedHashMap<>();
        private Duration idleTimeout = Duration.ofSeconds(30);
        private long maxConnectionBufferBytes = 128L * 1024 * 1024;
        private long maxStreamBufferBytes = 64L * 1024 * 1024;
        private int maxSessionsPerConnection = 1;
        private int maxBidiPerSession = 8;
        private int maxUniPerSession = 8;
        private int maxBufferedStreams = 16;

        @Override
        public WebTransportServer.Builder bind(String host, int port) {
            if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank");
            if (port < 0 || port > 65535) throw new IllegalArgumentException("port must be between 0 and 65535");
            this.host = host.trim();
            this.port = port;
            return this;
        }

        @Override
        public WebTransportServer.Builder tls(KeyStore keyStore, String certificateAlias, char[] privateKeyPassword) {
            this.keyStore = Objects.requireNonNull(keyStore, "keyStore");
            if (certificateAlias == null || certificateAlias.isBlank()) {
                throw new IllegalArgumentException("certificateAlias must not be blank");
            }
            this.certificateAlias = certificateAlias;
            this.privateKeyPassword = privateKeyPassword == null ? new char[0] : privateKeyPassword.clone();
            return this;
        }

        @Override
        public WebTransportServer.Builder allowedOrigins(Collection<String> origins) {
            allowedOrigins.clear();
            if (origins != null) {
                origins.stream()
                    .filter(Objects::nonNull)
                    .map(String::trim)
                    .filter(value -> !value.isEmpty())
                    .forEach(allowedOrigins::add);
            }
            return this;
        }

        @Override
        public WebTransportServer.Builder route(String path, WebTransportHandler handler) {
            if (path == null || path.isBlank() || !path.startsWith("/")) {
                throw new IllegalArgumentException("WebTransport route must be an absolute path beginning with '/'.");
            }
            Objects.requireNonNull(handler, "handler");
            WebTransportHandler previous = routes.get(path);
            if (previous != null && previous != handler) {
                throw new IllegalArgumentException("WebTransport route is already registered: " + path);
            }
            routes.put(path, handler);
            return this;
        }

        @Override
        public WebTransportServer.Builder idleTimeout(Duration timeout) {
            if (timeout == null || timeout.isZero() || timeout.isNegative()) {
                throw new IllegalArgumentException("idleTimeout must be positive");
            }
            idleTimeout = timeout;
            return this;
        }

        @Override
        public WebTransportServer.Builder maxConnectionBufferBytes(long bytes) {
            maxConnectionBufferBytes = positive(bytes, "maxConnectionBufferBytes");
            return this;
        }

        @Override
        public WebTransportServer.Builder maxStreamBufferBytes(long bytes) {
            maxStreamBufferBytes = positive(bytes, "maxStreamBufferBytes");
            return this;
        }

        @Override
        public WebTransportServer.Builder maxSessionsPerConnection(int count) {
            positive(count, "maxSessionsPerConnection");
            if (count != 1) {
                throw new IllegalArgumentException(
                    "The bundled WebTransport implementation currently supports one session per HTTP/3 connection. "
                        + "Multiple pooled sessions require draft-16 session-level WT flow-control capsules."
                );
            }
            maxSessionsPerConnection = count;
            return this;
        }

        @Override
        public WebTransportServer.Builder maxBidirectionalStreamsPerSession(int count) {
            maxBidiPerSession = nonNegative(count, "maxBidirectionalStreamsPerSession");
            return this;
        }

        @Override
        public WebTransportServer.Builder maxUnidirectionalStreamsPerSession(int count) {
            maxUniPerSession = nonNegative(count, "maxUnidirectionalStreamsPerSession");
            return this;
        }

        @Override
        public WebTransportServer.Builder maxBufferedStreamsPerConnection(int count) {
            maxBufferedStreams = nonNegative(count, "maxBufferedStreamsPerConnection");
            return this;
        }

        @Override
        public WebTransportServer build() {
            if (keyStore == null) throw new IllegalStateException("tls(...) must be configured before build().");
            if (routes.isEmpty()) throw new IllegalStateException("At least one WebTransport route must be registered.");
            return new KwikWebTransportServer(this);
        }

        private static int positive(int value, String name) {
            if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
            return value;
        }

        private static long positive(long value, String name) {
            if (value <= 0) throw new IllegalArgumentException(name + " must be positive");
            return value;
        }

        private static int nonNegative(int value, String name) {
            if (value < 0) throw new IllegalArgumentException(name + " must not be negative");
            return value;
        }

    }
}
