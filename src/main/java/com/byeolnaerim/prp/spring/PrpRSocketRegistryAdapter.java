package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.profile.rpc.PayloadCodec;
import io.netty.buffer.ByteBuf;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.DefaultPayload;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Publisher;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** RSocket 1.0 compatibility adapter backed by the same @PrpRoute registry as native PRP. */
public final class PrpRSocketRegistryAdapter {
    private static final String COMPOSITE_METADATA = "message/x.rsocket.composite-metadata.v0";
    private static final String ROUTING_METADATA = "message/x.rsocket.routing.v0";
    private static final int WELL_KNOWN_ROUTING = 0x7e;

    private final PrpRouteRegistry registry;
    private final PrpRouteInvoker invoker;
    private final PayloadCodec codec;

    public PrpRSocketRegistryAdapter(PrpRouteRegistry registry, PrpRouteInvoker invoker, PayloadCodec codec) {
        this.registry = java.util.Objects.requireNonNull(registry);
        this.invoker = java.util.Objects.requireNonNull(invoker);
        this.codec = java.util.Objects.requireNonNull(codec);
    }

    public SocketAcceptor acceptor(String transport) {
        return (setup, sendingSocket) -> {
            validateSetup(setup);
            String metadataMime = setup.metadataMimeType();
            return Mono.just(new RegistryResponder(transport, metadataMime, sendingSocket));
        };
    }

    private void validateSetup(ConnectionSetupPayload setup) {
        if (!codec.id().equals(setup.dataMimeType())) {
            throw new IllegalArgumentException("RSocket data MIME mismatch: expected " + codec.id() + ", received " + setup.dataMimeType() + ".");
        }
        if (!COMPOSITE_METADATA.equals(setup.metadataMimeType()) && !ROUTING_METADATA.equals(setup.metadataMimeType())) {
            throw new IllegalArgumentException("Unsupported RSocket metadata MIME: " + setup.metadataMimeType());
        }
    }

    private final class RegistryResponder implements RSocket {
        private final String transport;
        private final String metadataMime;
        private final RSocket sendingSocket;

        private RegistryResponder(String transport, String metadataMime, RSocket sendingSocket) {
            this.transport = transport;
            this.metadataMime = metadataMime;
            this.sendingSocket = sendingSocket;
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            try {
                String route = route(payload, metadataMime);
                PrpRouteDescriptor descriptor = registry.require(route, PrpInteraction.REQUEST_RESPONSE);
                Object input = decodeAndRelease(payload, descriptor.inputType());
                PrpContext context = new RSocketContext(route, PrpInteraction.REQUEST_RESPONSE, transport, sendingSocket, metadataMime);
                return invoker.requestResponse(descriptor, input, context).map(value -> DefaultPayload.create(codec.encode(value)));
            } catch (Throwable error) {
                safeRelease(payload);
                return Mono.error(error);
            }
        }

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            try {
                String route = route(payload, metadataMime);
                PrpRouteDescriptor descriptor = registry.require(route, PrpInteraction.FIRE_AND_FORGET);
                Object input = decodeAndRelease(payload, descriptor.inputType());
                PrpContext context = new RSocketContext(route, PrpInteraction.FIRE_AND_FORGET, transport, sendingSocket, metadataMime);
                return invoker.fireAndForget(descriptor, input, context);
            } catch (Throwable error) {
                safeRelease(payload);
                return Mono.error(error);
            }
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            try {
                String route = route(payload, metadataMime);
                PrpRouteDescriptor descriptor = registry.require(route, PrpInteraction.REQUEST_STREAM);
                Object input = decodeAndRelease(payload, descriptor.inputType());
                PrpContext context = new RSocketContext(route, PrpInteraction.REQUEST_STREAM, transport, sendingSocket, metadataMime);
                return invoker.requestStream(descriptor, input, context).map(value -> DefaultPayload.create(codec.encode(value)));
            } catch (Throwable error) {
                safeRelease(payload);
                return Flux.error(error);
            }
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(payloads).switchOnFirst((signal, flux) -> {
                if (!signal.hasValue()) {
                    Throwable error = signal.getThrowable();
                    return error == null ? Flux.empty() : Flux.error(error);
                }
                Payload first = signal.get();
                try {
                    String route = route(first, metadataMime);
                    PrpRouteDescriptor descriptor = registry.require(route, PrpInteraction.REQUEST_CHANNEL);
                    Flux<Object> decoded = flux.map(payload -> decodeAndRelease(payload, descriptor.inputType()));
                    PrpContext context = new RSocketContext(route, PrpInteraction.REQUEST_CHANNEL, transport, sendingSocket, metadataMime);
                    return invoker.requestChannel(descriptor, decoded, context).map(value -> DefaultPayload.create(codec.encode(value)));
                } catch (Throwable error) {
                    safeRelease(first);
                    return Flux.error(error);
                }
            });
        }
    }

    private final class RSocketContext implements PrpContext {
        private final String route;
        private final PrpInteraction interaction;
        private final String transport;
        private final PrpRequester requester;

        private RSocketContext(String route, PrpInteraction interaction, String transport, RSocket sendingSocket, String metadataMime) {
            this.route = route;
            this.interaction = interaction;
            this.transport = transport;
            this.requester = new RSocketPrpRequester(sendingSocket, metadataMime);
        }

        @Override public String route() { return route; }
        @Override public PrpInteraction interaction() { return interaction; }
        @Override public PrpWireProtocol wireProtocol() { return PrpWireProtocol.RSOCKET_1; }
        @Override public String transport() { return transport; }
        @Override public PrpRequester requester() { return requester; }
        @Override public Optional<ReactiveSession> nativeSession() { return Optional.empty(); }
        @Override public boolean datagramsAvailable() { return false; }
        @Override public CompletionStage<Void> sendDatagram(String route, byte[] data) {
            return CompletableFuture.failedFuture(new IllegalStateException("Native PRP datagrams are not available through RSocket compatibility."));
        }
        @Override public CompletionStage<Void> sendLatestDatagram(String route, byte[] data) {
            return sendDatagram(route, data);
        }
    }

    private final class RSocketPrpRequester implements PrpRequester {
        private final RSocket socket;
        private final String metadataMime;

        private RSocketPrpRequester(RSocket socket, String metadataMime) {
            this.socket = socket;
            this.metadataMime = metadataMime;
        }

        @Override
        public <I, O> CompletionStage<O> requestResponse(String route, I input, Class<O> responseType) {
            return socket.requestResponse(requestPayload(route, input, metadataMime))
                .map(payload -> decodeAndRelease(payload, responseType))
                .toFuture();
        }

        @Override
        public <I> CompletionStage<Void> fireAndForget(String route, I input) {
            return socket.fireAndForget(requestPayload(route, input, metadataMime)).toFuture();
        }

        @Override
        public <I, O> CompletionStage<Flow.Publisher<O>> requestStream(String route, I input, Class<O> responseType) {
            Flow.Publisher<O> publisher = JdkFlowAdapter.publisherToFlowPublisher(
                socket.requestStream(requestPayload(route, input, metadataMime)).map(payload -> decodeAndRelease(payload, responseType))
            );
            return CompletableFuture.completedFuture(publisher);
        }

        @Override
        public <I, O> CompletionStage<Flow.Publisher<O>> requestChannel(String route, Flow.Publisher<I> input, Class<O> responseType) {
            AtomicBoolean first = new AtomicBoolean(true);
            Flux<Payload> payloads = JdkFlowAdapter.flowPublisherToFlux(input).map(value -> {
                if (first.getAndSet(false)) return requestPayload(route, value, metadataMime);
                return DefaultPayload.create(codec.encode(value));
            });
            Flow.Publisher<O> output = JdkFlowAdapter.publisherToFlowPublisher(
                socket.requestChannel(payloads).map(payload -> decodeAndRelease(payload, responseType))
            );
            return CompletableFuture.completedFuture(output);
        }
    }

    private Payload requestPayload(String route, Object value, String metadataMime) {
        byte[] metadata = ROUTING_METADATA.equals(metadataMime) ? encodeRoute(route) : encodeCompositeRoute(route);
        return DefaultPayload.create(codec.encode(value), metadata);
    }

    private <T> T decodeAndRelease(Payload payload, Class<T> type) {
        try {
            return codec.decode(copy(payload.sliceData()), type);
        } finally {
            safeRelease(payload);
        }
    }

    private static String route(Payload payload, String metadataMime) {
        if (!payload.hasMetadata()) throw new IllegalArgumentException("RSocket request is missing routing metadata.");
        byte[] metadata = copy(payload.sliceMetadata());
        if (ROUTING_METADATA.equals(metadataMime)) return firstRoute(metadata, 0, metadata.length);
        if (!COMPOSITE_METADATA.equals(metadataMime)) throw new IllegalArgumentException("Unsupported RSocket metadata MIME: " + metadataMime);

        int offset = 0;
        while (offset < metadata.length) {
            int first = metadata[offset++] & 0xff;
            boolean known = (first & 0x80) != 0;
            int knownId = -1;
            String explicitMime = null;
            if (known) {
                knownId = first & 0x7f;
            } else {
                int mimeLength = (first & 0x7f) + 1;
                if (offset + mimeLength > metadata.length) throw new IllegalArgumentException("Malformed RSocket composite metadata MIME.");
                explicitMime = new String(metadata, offset, mimeLength, StandardCharsets.UTF_8);
                offset += mimeLength;
            }
            if (offset + 3 > metadata.length) throw new IllegalArgumentException("Malformed RSocket composite metadata length.");
            int contentLength = ((metadata[offset] & 0xff) << 16) | ((metadata[offset + 1] & 0xff) << 8) | (metadata[offset + 2] & 0xff);
            offset += 3;
            if (offset + contentLength > metadata.length) throw new IllegalArgumentException("Truncated RSocket composite metadata entry.");
            if (knownId == WELL_KNOWN_ROUTING || ROUTING_METADATA.equals(explicitMime)) {
                return firstRoute(metadata, offset, contentLength);
            }
            offset += contentLength;
        }
        throw new IllegalArgumentException("RSocket request does not contain routing metadata.");
    }

    private static String firstRoute(byte[] bytes, int offset, int length) {
        if (length < 1) throw new IllegalArgumentException("RSocket routing metadata is empty.");
        int routeLength = bytes[offset] & 0xff;
        if (routeLength == 0 || routeLength + 1 > length) throw new IllegalArgumentException("Malformed RSocket routing metadata.");
        return new String(bytes, offset + 1, routeLength, StandardCharsets.UTF_8);
    }

    private static byte[] encodeRoute(String route) {
        byte[] value = route.getBytes(StandardCharsets.UTF_8);
        if (value.length == 0 || value.length > 255) throw new IllegalArgumentException("RSocket route must encode to 1..255 UTF-8 bytes.");
        byte[] output = new byte[value.length + 1];
        output[0] = (byte) value.length;
        System.arraycopy(value, 0, output, 1, value.length);
        return output;
    }

    private static byte[] encodeCompositeRoute(String route) {
        byte[] content = encodeRoute(route);
        byte[] output = new byte[1 + 3 + content.length];
        output[0] = (byte) (0x80 | WELL_KNOWN_ROUTING);
        int length = content.length;
        output[1] = (byte) (length >>> 16);
        output[2] = (byte) (length >>> 8);
        output[3] = (byte) length;
        System.arraycopy(content, 0, output, 4, content.length);
        return output;
    }

    private static byte[] copy(ByteBuf buffer) {
        byte[] output = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), output);
        return output;
    }

    private static void safeRelease(Payload payload) {
        if (payload == null) return;
        try { if (payload.refCnt() > 0) payload.release(); } catch (Throwable ignored) { }
    }
}
