package com.byeolnaerim.prp.transport;

import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.error.ConnectionLostException;
import com.byeolnaerim.prp.error.TransportUnavailableException;
import com.byeolnaerim.prp.internal.BoundedPublisher;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/** Reliable ordered byte-stream adapter using the PRP/1 unsigned 32-bit outer frame length. */
public final class JdkByteStreamTransport implements ReactiveTransport {
    @FunctionalInterface
    public interface Opener {
        Endpoint open() throws Exception;
    }

    public interface Endpoint extends AutoCloseable {
        InputStream input();
        OutputStream output();
        @Override void close() throws Exception;
    }

    private final String id;
    private final List<String> traits;
    private final Opener opener;

    public JdkByteStreamTransport(String id, List<String> traits, Opener opener) {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("Transport id must not be blank.");
        this.id = id;
        this.traits = List.copyOf(traits == null ? List.of() : traits);
        this.opener = Objects.requireNonNull(opener);
    }

    public static JdkByteStreamTransport from(String id, Endpoint endpoint, String... traits) {
        Objects.requireNonNull(endpoint);
        return new JdkByteStreamTransport(id, List.of(traits), () -> endpoint);
    }

    @Override public String id() { return id; }

    @Override
    public CompletionStage<TransportConnection> connect() {
        CompletableFuture<TransportConnection> result = new CompletableFuture<>();
        Thread.ofVirtual().name("prp-byte-stream-connect").start(() -> {
            try { result.complete(new Connection(id, traits, opener.open())); }
            catch (Throwable error) { result.completeExceptionally(new TransportUnavailableException("Unable to open " + id + " transport: " + error.getMessage(), error)); }
        });
        return result;
    }

    private static final class Connection implements TransportConnection {
        private final String id;
        private final List<String> traits;
        private final Endpoint endpoint;
        private final DataInputStream input;
        private final DataOutputStream output;
        private final CompletableFuture<TransportCloseEvent> closed = new CompletableFuture<>();
        private final AtomicBoolean laneOpened = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();
        private final Object writeLock = new Object();
        private volatile BoundedPublisher<byte[]> incoming;

        private Connection(String id, List<String> traits, Endpoint endpoint) {
            this.id = id;
            this.traits = traits;
            this.endpoint = Objects.requireNonNull(endpoint);
            this.input = new DataInputStream(Objects.requireNonNull(endpoint.input()));
            this.output = new DataOutputStream(Objects.requireNonNull(endpoint.output()));
        }

        @Override
        public TransportDescription description() {
            return new TransportDescription(id, java.util.stream.Stream.concat(
                java.util.stream.Stream.of("reliable", "ordered", "byte-stream", "prp-u32-framing"),
                traits.stream()
            ).distinct().toList());
        }

        @Override
        public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
            if (!"reliable".equals(requirements.reliability()) || !"ordered".equals(requirements.ordering())) return CompletableFuture.failedFuture(new IllegalArgumentException("PRP byte-stream transport requires a reliable ordered lane."));
            if (!laneOpened.compareAndSet(false, true)) return CompletableFuture.failedFuture(new IllegalStateException("Byte-stream connection exposes one base lane."));
            int maxFrameBytes = Math.max(requirements.maxFrameBytes(), ProtocolLimits.BOOTSTRAP.maxFrameBytes());
            BoundedPublisher<byte[]> queue = new BoundedPublisher<>(256);
            incoming = queue;
            Thread.ofVirtual().name("prp-byte-stream-reader").start(() -> readLoop(queue, maxFrameBytes));
            return CompletableFuture.completedFuture(new TransportLane() {
                @Override public String id() { return id + ":0"; }
                @Override public java.util.concurrent.Flow.Publisher<byte[]> incoming() { return queue; }
                @Override public CompletionStage<Void> write(byte[] frame) { return writeFrame(frame, maxFrameBytes); }
                @Override public CompletionStage<Void> close(String reason) { return Connection.this.close(null, reason); }
            });
        }

        private void readLoop(BoundedPublisher<byte[]> queue, int maxFrameBytes) {
            try {
                while (!ended.get()) {
                    int length;
                    try { length = input.readInt(); }
                    catch (EOFException eof) { finish(new TransportCloseEvent(null, "remote-close"), null); return; }
                    long unsignedLength = Integer.toUnsignedLong(length);
                    if (unsignedLength == 0 || unsignedLength > maxFrameBytes || unsignedLength > Integer.MAX_VALUE) {
                        throw new ConnectionLostException("PRP byte-stream frame length " + unsignedLength + " exceeds the admitted bound " + maxFrameBytes + ".");
                    }
                    byte[] frame = input.readNBytes((int) unsignedLength);
                    if (frame.length != (int) unsignedLength) throw new EOFException("Truncated PRP byte-stream frame.");
                    if (!queue.emitWait(frame)) return;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                finish(null, interrupted);
            } catch (Throwable error) {
                finish(null, error);
            }
        }

        private CompletionStage<Void> writeFrame(byte[] frame, int maxFrameBytes) {
            if (frame == null) return CompletableFuture.failedFuture(new NullPointerException("frame"));
            if (frame.length == 0 || frame.length > maxFrameBytes) return CompletableFuture.failedFuture(new IllegalArgumentException("PRP frame must be 1.." + maxFrameBytes + " bytes."));
            if (ended.get()) return CompletableFuture.failedFuture(new ConnectionLostException());
            CompletableFuture<Void> result = new CompletableFuture<>();
            Thread.ofVirtual().name("prp-byte-stream-writer").start(() -> {
                try {
                    synchronized (writeLock) {
                        if (ended.get()) throw new ConnectionLostException();
                        output.writeInt(frame.length);
                        output.write(frame);
                        output.flush();
                    }
                    result.complete(null);
                } catch (Throwable error) {
                    finish(null, error);
                    result.completeExceptionally(error);
                }
            });
            return result;
        }

        @Override public CompletionStage<TransportCloseEvent> closed() { return closed; }

        @Override
        public CompletionStage<Void> close(Integer code, String reason) {
            finish(new TransportCloseEvent(code, reason), null);
            return CompletableFuture.completedFuture(null);
        }

        private void finish(TransportCloseEvent event, Throwable error) {
            if (!ended.compareAndSet(false, true)) return;
            try { endpoint.close(); } catch (Exception ignored) {}
            BoundedPublisher<byte[]> queue = incoming;
            if (error == null) {
                if (queue != null) queue.complete();
                closed.complete(event == null ? new TransportCloseEvent(null, "closed") : event);
            } else {
                if (queue != null) queue.fail(error);
                closed.completeExceptionally(error instanceof IOException ? new ConnectionLostException(error.getMessage(), error) : error);
            }
        }
    }
}
