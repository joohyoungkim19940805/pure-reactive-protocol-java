package com.byeolnaerim.prp.transport;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

/** JDK TCP/TLS transport. PRP framing is delegated to {@link JdkByteStreamTransport}. */
public final class JdkTcpTransport implements ReactiveTransport {
    private final JdkByteStreamTransport delegate;

    public JdkTcpTransport(String host, int port) {
        this(host, port, Duration.ofSeconds(10), SocketFactory.getDefault(), false);
    }

    public JdkTcpTransport(String host, int port, Duration connectTimeout) {
        this(host, port, connectTimeout, SocketFactory.getDefault(), false);
    }

    private JdkTcpTransport(String host, int port, Duration connectTimeout, SocketFactory socketFactory, boolean tls) {
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host must not be blank.");
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("port must be 1..65535.");
        if (connectTimeout == null || connectTimeout.isNegative() || connectTimeout.isZero()) throw new IllegalArgumentException("connectTimeout must be positive.");
        delegate = new JdkByteStreamTransport(tls ? "tcp+tls" : "tcp", tls ? List.of("tcp", "tls") : List.of("tcp"), () -> {
            Socket socket = socketFactory.createSocket();
            socket.connect(new InetSocketAddress(host, port), Math.toIntExact(Math.min(Integer.MAX_VALUE, connectTimeout.toMillis())));
            socket.setTcpNoDelay(true);
            return endpoint(socket);
        });
    }

    public static JdkTcpTransport tls(String host, int port) {
        return new JdkTcpTransport(host, port, Duration.ofSeconds(10), SSLSocketFactory.getDefault(), true);
    }

    public static JdkTcpTransport tls(String host, int port, Duration connectTimeout) {
        return new JdkTcpTransport(host, port, connectTimeout, SSLSocketFactory.getDefault(), true);
    }

    public static JdkByteStreamTransport accepted(Socket socket) {
        if (socket == null) throw new NullPointerException("socket");
        return JdkByteStreamTransport.from("tcp", endpoint(socket), "tcp", "accepted");
    }

    @Override public String id() { return delegate.id(); }
    @Override public java.util.concurrent.CompletionStage<TransportConnection> connect() { return delegate.connect(); }

    private static JdkByteStreamTransport.Endpoint endpoint(Socket socket) {
        return new JdkByteStreamTransport.Endpoint() {
            @Override public InputStream input() { try { return socket.getInputStream(); } catch (java.io.IOException error) { throw new IllegalStateException(error); } }
            @Override public OutputStream output() { try { return socket.getOutputStream(); } catch (java.io.IOException error) { throw new IllegalStateException(error); } }
            @Override public void close() throws Exception { socket.close(); }
        };
    }
}
