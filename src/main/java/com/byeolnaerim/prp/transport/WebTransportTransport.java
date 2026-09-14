package com.byeolnaerim.prp.transport;

import com.byeolnaerim.prp.core.ProtocolLimits;
import com.byeolnaerim.prp.error.ConnectionLostException;
import com.byeolnaerim.prp.error.TransportUnavailableException;
import com.byeolnaerim.prp.internal.BoundedPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

/** Native PRP over WebTransport: reliable PRP/1 stream plus an optional native best-effort datagram lane. */
public final class WebTransportTransport implements ReactiveTransport {
    @FunctionalInterface
    public interface Opener {
        CompletionStage<Endpoint> open();
    }

    /** Runtime-neutral view of one accepted or connected WebTransport session. */
    public interface Endpoint {
        Flow.Publisher<byte[]> incoming();
        CompletionStage<Void> write(byte[] bytes);
        CompletionStage<Void> close(String reason);
        default DatagramEndpoint datagrams() { return null; }
    }

    /** Runtime-neutral WebTransport datagram endpoint. Bytes are one complete WebTransport datagram payload. */
    public interface DatagramEndpoint {
        Flow.Publisher<byte[]> incoming();
        CompletionStage<Void> write(byte[] bytes);
        int maxDatagramBytes();
        default CompletionStage<Void> close(String reason) { return CompletableFuture.completedFuture(null); }
    }

    private final Opener opener;

    public WebTransportTransport(Opener opener) {
        this.opener = Objects.requireNonNull(opener);
    }

    public static WebTransportTransport from(Endpoint endpoint) {
        Objects.requireNonNull(endpoint);
        return new WebTransportTransport(() -> CompletableFuture.completedFuture(endpoint));
    }

    @Override public String id() { return "webtransport"; }

    @Override
    public CompletionStage<TransportConnection> connect() {
        CompletableFuture<TransportConnection> result = new CompletableFuture<>();
        try {
            opener.open().whenComplete((endpoint, error) -> {
                if (error != null) {
                    result.completeExceptionally(new TransportUnavailableException(
                        "Unable to open WebTransport session: " + error.getMessage(), error
                    ));
                } else {
                    try {
                        result.complete(new Connection(Objects.requireNonNull(endpoint, "WebTransport opener returned null.")));
                    } catch (Throwable invalidEndpoint) {
                        result.completeExceptionally(new TransportUnavailableException(
                            "Unable to open WebTransport session: " + invalidEndpoint.getMessage(), invalidEndpoint
                        ));
                    }
                }
            });
        } catch (Throwable error) {
            result.completeExceptionally(new TransportUnavailableException(
                "Unable to open WebTransport session: " + error.getMessage(), error
            ));
        }
        return result;
    }

    private static final class Connection implements TransportConnection {
        private final Endpoint endpoint;
        private final DatagramEndpoint datagrams;
        private final CompletableFuture<TransportCloseEvent> closed = new CompletableFuture<>();
        private final AtomicBoolean reliableLaneOpened = new AtomicBoolean();
        private final AtomicBoolean datagramLaneOpened = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();
        private volatile BoundedPublisher<byte[]> reliableIncoming;
        private volatile BoundedPublisher<byte[]> datagramIncoming;
        private volatile Flow.Subscription reliableInputSubscription;
        private volatile Flow.Subscription datagramInputSubscription;

        private Connection(Endpoint endpoint) {
            this.endpoint = endpoint;
            this.datagrams = endpoint.datagrams();
        }

        @Override
        public TransportDescription description() {
            List<String> traits = new ArrayList<>(List.of(
                "reliable", "ordered", "byte-stream", "prp-u32-framing",
                "webtransport", "http3", "quic", "single-bidirectional-stream"
            ));
            if (datagrams != null && datagrams.maxDatagramBytes() > 0) traits.addAll(List.of("best-effort", "unordered", "datagrams"));
            return new TransportDescription("webtransport", List.copyOf(traits));
        }

        @Override
        public boolean supportsLane(LaneRequirements requirements) {
            if (isReliable(requirements)) return true;
            return isDatagram(requirements) && datagrams != null && datagrams.maxDatagramBytes() > 0;
        }

        @Override
        public CompletionStage<TransportLane> openLane(LaneRequirements requirements) {
            if (isReliable(requirements)) return openReliableLane(requirements);
            if (isDatagram(requirements)) return openDatagramLane(requirements);
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "PRP WebTransport supports reliable/ordered and best-effort/unordered lanes only."
            ));
        }

        private CompletionStage<TransportLane> openReliableLane(LaneRequirements requirements) {
            if (!reliableLaneOpened.compareAndSet(false, true)) {
                return CompletableFuture.failedFuture(new IllegalStateException("PRP WebTransport connection exposes one reliable base lane."));
            }
            int maxFrameBytes = Math.max(requirements.maxFrameBytes(), ProtocolLimits.BOOTSTRAP.maxFrameBytes());
            BoundedPublisher<byte[]> queue = new BoundedPublisher<>(256);
            LengthDecoder decoder = new LengthDecoder(maxFrameBytes);
            reliableIncoming = queue;

            endpoint.incoming().subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    if (reliableInputSubscription != null) { subscription.cancel(); return; }
                    reliableInputSubscription = subscription;
                    subscription.request(1);
                }
                @Override
                public void onNext(byte[] chunk) {
                    if (ended.get()) return;
                    try {
                        for (byte[] frame : decoder.push(Objects.requireNonNull(chunk))) {
                            if (!queue.emit(frame)) throw new ConnectionLostException("PRP WebTransport incoming frame queue overflow.");
                        }
                        Flow.Subscription subscription = reliableInputSubscription;
                        if (subscription != null && !ended.get()) subscription.request(1);
                    } catch (Throwable error) {
                        Flow.Subscription subscription = reliableInputSubscription;
                        if (subscription != null) subscription.cancel();
                        finish(null, error);
                    }
                }
                @Override public void onError(Throwable error) { finish(null, error); }
                @Override public void onComplete() { finish(new TransportCloseEvent(null, "remote-close"), null); }
            });

            return CompletableFuture.completedFuture(new TransportLane() {
                @Override public String id() { return "webtransport:reliable"; }
                @Override public int maxFrameBytes() { return maxFrameBytes; }
                @Override public Flow.Publisher<byte[]> incoming() { return queue; }
                @Override
                public CompletionStage<Void> write(byte[] frame) {
                    if (frame == null) return CompletableFuture.failedFuture(new NullPointerException("frame"));
                    if (frame.length == 0 || frame.length > maxFrameBytes) {
                        return CompletableFuture.failedFuture(new IllegalArgumentException("PRP frame must be 1.." + maxFrameBytes + " bytes."));
                    }
                    if (ended.get()) return CompletableFuture.failedFuture(new ConnectionLostException());
                    byte[] encoded = new byte[4 + frame.length];
                    encoded[0] = (byte) (frame.length >>> 24);
                    encoded[1] = (byte) (frame.length >>> 16);
                    encoded[2] = (byte) (frame.length >>> 8);
                    encoded[3] = (byte) frame.length;
                    System.arraycopy(frame, 0, encoded, 4, frame.length);
                    try {
                        return Objects.requireNonNull(endpoint.write(encoded), "WebTransport write returned null.")
                            .whenComplete((ignored, error) -> { if (error != null) finish(null, error); });
                    } catch (Throwable error) {
                        finish(null, error);
                        return CompletableFuture.failedFuture(error);
                    }
                }
                @Override public CompletionStage<Void> close(String reason) { return Connection.this.close(null, reason); }
            });
        }

        private CompletionStage<TransportLane> openDatagramLane(LaneRequirements requirements) {
            if (datagrams == null) return CompletableFuture.failedFuture(new IllegalArgumentException("This WebTransport endpoint does not expose datagrams."));
            if (!datagramLaneOpened.compareAndSet(false, true)) {
                return CompletableFuture.failedFuture(new IllegalStateException("PRP WebTransport connection exposes one datagram lane."));
            }
            int carrierMax = datagrams.maxDatagramBytes();
            if (carrierMax <= 0) return CompletableFuture.failedFuture(new IllegalStateException("WebTransport datagram size is unavailable."));
            int maxFrameBytes = Math.min(carrierMax, requirements.maxFrameBytes());
            if (maxFrameBytes <= 0) return CompletableFuture.failedFuture(new IllegalArgumentException("WebTransport datagram lane has no usable payload size."));
            BoundedPublisher<byte[]> queue = new BoundedPublisher<>(256);
            datagramIncoming = queue;

            datagrams.incoming().subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    if (datagramInputSubscription != null) { subscription.cancel(); return; }
                    datagramInputSubscription = subscription;
                    subscription.request(1);
                }
                @Override
                public void onNext(byte[] frame) {
                    if (ended.get()) return;
                    if (frame != null && frame.length <= maxFrameBytes) queue.emit(frame.clone());
                    Flow.Subscription subscription = datagramInputSubscription;
                    if (subscription != null && !ended.get()) subscription.request(1);
                }
                @Override public void onError(Throwable error) { queue.fail(error); }
                @Override public void onComplete() { queue.complete(); }
            });

            return CompletableFuture.completedFuture(new TransportLane() {
                @Override public String id() { return "webtransport:datagram"; }
                @Override public int maxFrameBytes() { return maxFrameBytes; }
                @Override public Flow.Publisher<byte[]> incoming() { return queue; }
                @Override
                public CompletionStage<Void> write(byte[] frame) {
                    if (frame == null) return CompletableFuture.failedFuture(new NullPointerException("frame"));
                    if (frame.length > maxFrameBytes) return CompletableFuture.failedFuture(new IllegalArgumentException("WebTransport datagram exceeds " + maxFrameBytes + " bytes."));
                    if (ended.get()) return CompletableFuture.failedFuture(new ConnectionLostException());
                    try { return Objects.requireNonNull(datagrams.write(frame.clone()), "WebTransport datagram write returned null."); }
                    catch (Throwable error) { return CompletableFuture.failedFuture(error); }
                }
                @Override
                public CompletionStage<Void> close(String reason) {
                    Flow.Subscription subscription = datagramInputSubscription;
                    if (subscription != null) subscription.cancel();
                    queue.complete();
                    try { return Objects.requireNonNull(datagrams.close(reason)); }
                    catch (Throwable error) { return CompletableFuture.completedFuture(null); }
                }
            });
        }

        @Override public CompletionStage<TransportCloseEvent> closed() { return closed; }

        @Override
        public CompletionStage<Void> close(Integer code, String reason) {
            finish(new TransportCloseEvent(code, reason), null);
            CompletionStage<Void> datagramClose;
            try { datagramClose = datagrams == null ? CompletableFuture.completedFuture(null) : datagrams.close(reason).exceptionally(error -> null); }
            catch (Throwable error) { datagramClose = CompletableFuture.completedFuture(null); }
            return datagramClose.thenCompose(ignored -> {
                try { return Objects.requireNonNull(endpoint.close(reason), "WebTransport close returned null.").exceptionally(error -> null); }
                catch (Throwable error) { return CompletableFuture.completedFuture(null); }
            });
        }

        private void finish(TransportCloseEvent event, Throwable error) {
            if (!ended.compareAndSet(false, true)) return;
            Flow.Subscription reliableSubscription = reliableInputSubscription;
            if (reliableSubscription != null) reliableSubscription.cancel();
            Flow.Subscription datagramSubscription = datagramInputSubscription;
            if (datagramSubscription != null) datagramSubscription.cancel();
            BoundedPublisher<byte[]> reliableQueue = reliableIncoming;
            BoundedPublisher<byte[]> datagramQueue = datagramIncoming;
            if (error == null) {
                if (reliableQueue != null) reliableQueue.complete();
                if (datagramQueue != null) datagramQueue.complete();
                closed.complete(event == null ? new TransportCloseEvent(null, "closed") : event);
            } else {
                ConnectionLostException connectionError = error instanceof ConnectionLostException connectionLost
                    ? connectionLost
                    : new ConnectionLostException(error.getMessage(), error);
                if (reliableQueue != null) reliableQueue.fail(connectionError);
                if (datagramQueue != null) datagramQueue.fail(connectionError);
                closed.completeExceptionally(connectionError);
                try { endpoint.close("stream-error"); } catch (Throwable ignored) { }
            }
        }

        private static boolean isReliable(LaneRequirements requirements) {
            return "reliable".equals(requirements.reliability()) && "ordered".equals(requirements.ordering());
        }

        private static boolean isDatagram(LaneRequirements requirements) {
            return "best-effort".equals(requirements.reliability()) && "unordered".equals(requirements.ordering());
        }
    }

    private static final class LengthDecoder {
        private final int maxFrameBytes;
        private final byte[] prefix = new byte[4];
        private int prefixBytes;
        private byte[] frame;
        private int frameBytes;

        private LengthDecoder(int maxFrameBytes) { this.maxFrameBytes = maxFrameBytes; }

        private List<byte[]> push(byte[] chunk) {
            List<byte[]> frames = new ArrayList<>();
            int offset = 0;
            while (offset < chunk.length) {
                if (frame == null) {
                    while (prefixBytes < 4 && offset < chunk.length) prefix[prefixBytes++] = chunk[offset++];
                    if (prefixBytes < 4) break;
                    long length = ((long) (prefix[0] & 0xff) << 24)
                        | ((long) (prefix[1] & 0xff) << 16)
                        | ((long) (prefix[2] & 0xff) << 8)
                        | (long) (prefix[3] & 0xff);
                    prefixBytes = 0;
                    if (length == 0 || length > maxFrameBytes || length > Integer.MAX_VALUE) {
                        throw new ConnectionLostException("PRP WebTransport frame length " + length + " exceeds the admitted bound " + maxFrameBytes + ".");
                    }
                    frame = new byte[(int) length];
                    frameBytes = 0;
                }
                int copy = Math.min(frame.length - frameBytes, chunk.length - offset);
                System.arraycopy(chunk, offset, frame, frameBytes, copy);
                frameBytes += copy;
                offset += copy;
                if (frameBytes == frame.length) {
                    frames.add(frame);
                    frame = null;
                    frameBytes = 0;
                }
            }
            return frames;
        }
    }
}
