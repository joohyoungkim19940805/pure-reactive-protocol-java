package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionState;
import com.byeolnaerim.prp.core.PrpRuntime;
import com.byeolnaerim.prp.profile.datagram.DatagramPeer;
import com.byeolnaerim.prp.profile.datagram.DatagramRoutingProfile;
import com.byeolnaerim.prp.profile.datagram.RoutedDatagram;
import com.byeolnaerim.prp.profile.rpc.PayloadCodec;
import com.byeolnaerim.prp.profile.rpc.RpcHandlers;
import com.byeolnaerim.prp.profile.rpc.RpcPeer;
import com.byeolnaerim.prp.profile.rpc.RpcProfile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Consumer;
import reactor.adapter.JdkFlowAdapter;

/** Adapts the transport-neutral Spring PRP registry to native PRP/1. */
public final class PrpSpringRuntimeAdapter {
    private final PrpRouteRegistry registry;
    private final PrpRouteInvoker invoker;
    private final PayloadCodec codec;

    public PrpSpringRuntimeAdapter(PrpRouteRegistry registry, PrpRouteInvoker invoker, PayloadCodec codec) {
        this.registry = java.util.Objects.requireNonNull(registry);
        this.invoker = java.util.Objects.requireNonNull(invoker);
        this.codec = java.util.Objects.requireNonNull(codec);
    }

    public PrpRuntime createRuntime(String transport) {
        return createRuntime(transport, builder -> {});
    }

    public PrpRuntime createRuntime(String transport, Consumer<PrpRuntime.Builder> customizer) {
        if (!registry.sealed()) throw new IllegalStateException("PRP route registry is not initialized yet.");
        RpcProfile rpc = new RpcProfile(codec);
        Map<String, List<PrpRouteDescriptor>> rpcRoutes = new LinkedHashMap<>();
        for (PrpRouteDescriptor descriptor : registry.routes()) {
            if (descriptor.interaction() == PrpInteraction.DATAGRAM) continue;
            rpcRoutes.computeIfAbsent(descriptor.route(), ignored -> new ArrayList<>()).add(descriptor);
        }
        for (List<PrpRouteDescriptor> descriptors : rpcRoutes.values()) {
            registerRpc(rpc, descriptors, transport);
        }

        PrpRuntime.Builder builder = PrpRuntime.builder().extension(rpc);
        List<String> datagramRoutes = registry.datagramRoutes();
        if (!datagramRoutes.isEmpty()) {
            builder.extension(new DatagramRoutingProfile(datagramRoutes.toArray(String[]::new)));
        }
        customizer.accept(builder);
        return builder.build();
    }

    public CompletionStage<Void> bindSession(ReactiveSession session, String transport) {
        List<PrpRouteDescriptor> routes = registry.routes(PrpInteraction.DATAGRAM);
        if (routes.isEmpty() || !session.supports(DatagramRoutingProfile.CAPABILITY_ID)) {
            return CompletableFuture.completedFuture(null);
        }

        DatagramPeer datagram = new DatagramPeer(session);
        RpcPeer rpc = new RpcPeer(session);
        List<AutoCloseable> registrations = new ArrayList<>();
        try {
            for (PrpRouteDescriptor descriptor : routes) {
                if (!datagram.supports(descriptor.route())) continue;
                registrations.add(datagram.route(descriptor.route(), packet -> {
                    PrpContext context = new NativeContext(
                        descriptor.route(),
                        PrpInteraction.DATAGRAM,
                        transport,
                        session,
                        rpc,
                        datagram
                    );
                    return invoker.datagram(descriptor, new RoutedDatagram(packet.route(), packet.data()), context).toFuture();
                }));
            }
        } catch (Throwable error) {
            closeAll(registrations);
            return CompletableFuture.failedFuture(error);
        }

        final AutoCloseable[] stateRegistration = new AutoCloseable[1];
        stateRegistration[0] = session.onStateChange(state -> {
            if (state != SessionState.CLOSED && state != SessionState.DETACHED) return;
            closeAll(registrations);
            closeQuietly(stateRegistration[0]);
        });
        return CompletableFuture.completedFuture(null);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void registerRpc(RpcProfile rpc, List<PrpRouteDescriptor> descriptors, String transport) {
        if (descriptors.isEmpty()) return;
        PrpRouteDescriptor first = descriptors.getFirst();
        RpcHandlers.Builder builder = RpcHandlers.builder((Class) first.inputType());
        for (PrpRouteDescriptor descriptor : descriptors) {
            switch (descriptor.interaction()) {
                case REQUEST_RESPONSE -> builder.requestResponse((input, rpcContext) ->
                    invoker.requestResponse(
                        descriptor,
                        input,
                        nativeContext(descriptor, transport, rpcContext.session())
                    ).toFuture()
                );
                case FIRE_AND_FORGET -> builder.fireAndForget((input, rpcContext) ->
                    invoker.fireAndForget(
                        descriptor,
                        input,
                        nativeContext(descriptor, transport, rpcContext.session())
                    ).toFuture()
                );
                case REQUEST_STREAM -> builder.requestStream((input, rpcContext) ->
                    JdkFlowAdapter.publisherToFlowPublisher(invoker.requestStream(
                        descriptor,
                        input,
                        nativeContext(descriptor, transport, rpcContext.session())
                    ))
                );
                case REQUEST_CHANNEL -> builder.requestChannel((input, rpcContext) ->
                    JdkFlowAdapter.publisherToFlowPublisher(invoker.requestChannel(
                        descriptor,
                        JdkFlowAdapter.flowPublisherToFlux((Flow.Publisher<Object>) input),
                        nativeContext(descriptor, transport, rpcContext.session())
                    ))
                );
                case DATAGRAM -> throw new IllegalStateException("DATAGRAM must not be registered in rpc/1.");
            }
        }
        rpc.register(first.route(), builder.build());
    }

    private PrpContext nativeContext(PrpRouteDescriptor descriptor, String transport, ReactiveSession session) {
        RpcPeer rpc = new RpcPeer(session);
        DatagramPeer datagram = session.supports(DatagramRoutingProfile.CAPABILITY_ID) ? new DatagramPeer(session) : null;
        return new NativeContext(descriptor.route(), descriptor.interaction(), transport, session, rpc, datagram);
    }

    private static final class NativeContext implements PrpContext {
        private final String route;
        private final PrpInteraction interaction;
        private final String transport;
        private final ReactiveSession session;
        private final RpcPeer rpc;
        private final DatagramPeer datagram;
        private final PrpRequester requester;

        private NativeContext(
            String route,
            PrpInteraction interaction,
            String transport,
            ReactiveSession session,
            RpcPeer rpc,
            DatagramPeer datagram
        ) {
            this.route = route;
            this.interaction = interaction;
            this.transport = transport;
            this.session = session;
            this.rpc = rpc;
            this.datagram = datagram;
            this.requester = new NativeRequester(rpc);
        }

        @Override public String route() { return route; }
        @Override public PrpInteraction interaction() { return interaction; }
        @Override public PrpWireProtocol wireProtocol() { return PrpWireProtocol.NATIVE; }
        @Override public String transport() { return transport; }
        @Override public PrpRequester requester() { return requester; }
        @Override public Optional<ReactiveSession> nativeSession() { return Optional.of(session); }
        @Override public boolean datagramsAvailable() { return datagram != null; }
        @Override public CompletionStage<Void> sendDatagram(String target, byte[] data) {
            return datagram == null
                ? CompletableFuture.failedFuture(new IllegalStateException("Native PRP datagram routing is unavailable for this session."))
                : datagram.send(target, data);
        }
        @Override public CompletionStage<Void> sendLatestDatagram(String target, byte[] data) {
            return datagram == null
                ? CompletableFuture.failedFuture(new IllegalStateException("Native PRP datagram routing is unavailable for this session."))
                : datagram.sendLatest(target, data);
        }
    }

    private record NativeRequester(RpcPeer peer) implements PrpRequester {
        @Override public <I, O> CompletionStage<O> requestResponse(String route, I input, Class<O> responseType) {
            return peer.requestResponse(route, input, responseType);
        }
        @Override public <I> CompletionStage<Void> fireAndForget(String route, I input) {
            return peer.fireAndForget(route, input);
        }
        @Override public <I, O> CompletionStage<Flow.Publisher<O>> requestStream(String route, I input, Class<O> responseType) {
            return peer.requestStream(route, input, responseType);
        }
        @Override public <I, O> CompletionStage<Flow.Publisher<O>> requestChannel(String route, Flow.Publisher<I> input, Class<O> responseType) {
            return peer.requestChannel(route, input, responseType);
        }
    }

    private static void closeAll(List<AutoCloseable> closeables) {
        for (AutoCloseable closeable : closeables) closeQuietly(closeable);
        closeables.clear();
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) return;
        try { closeable.close(); } catch (Exception ignored) { }
    }
}
