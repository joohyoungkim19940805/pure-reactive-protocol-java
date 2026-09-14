package com.byeolnaerim.prp.profile.datagram;

import com.byeolnaerim.prp.DatagramAcceptor;
import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.SessionDatagram;
import com.byeolnaerim.prp.core.CapabilityDescriptor;
import com.byeolnaerim.prp.core.DatagramCodec;
import com.byeolnaerim.prp.core.NegotiatedCapability;
import com.byeolnaerim.prp.core.ProtocolExtension;
import com.byeolnaerim.prp.error.CapabilityMismatchException;
import com.byeolnaerim.prp.internal.InternalAccess;
import com.byeolnaerim.prp.internal.SessionInternals;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DatagramRoutingProfile implements ProtocolExtension {
    public static final String CAPABILITY_ID = "prp.profile.datagram-routing";
    public static final String PROFILE_ID = "datagram-routing/1";

    private final List<String> routes;
    private final CapabilityDescriptor capability;

    public DatagramRoutingProfile(List<String> routes) {
        this.routes = DatagramRoutingCodec.normalize(routes);
        this.capability = new CapabilityDescriptor(CAPABILITY_ID, 1, 1, DatagramRoutingCodec.encodeRoutes(this.routes));
    }

    public DatagramRoutingProfile(String... routes) { this(List.of(routes)); }

    @Override public CapabilityDescriptor capability() { return capability; }
    @Override public Set<String> requiredCapabilities() { return Set.of(DatagramCodec.CAPABILITY_ID); }

    @Override
    public CompletionStage<AutoCloseable> attach(ReactiveSession session) {
        if (!session.supports(DatagramCodec.CAPABILITY_ID)) {
            return CompletableFuture.failedFuture(new CapabilityMismatchException("datagram-routing/1 requires negotiated prp.core.datagram."));
        }
        SessionInternals internals = InternalAccess.of(session);
        NegotiatedCapability negotiated = internals.capabilities().get(CAPABILITY_ID);
        if (negotiated == null) return CompletableFuture.failedFuture(new CapabilityMismatchException("datagram-routing/1 was attached without negotiation."));
        List<String> local = DatagramRoutingCodec.decodeRoutes(negotiated.local().parameters());
        Set<String> remote = new LinkedHashSet<>(DatagramRoutingCodec.decodeRoutes(negotiated.remote().parameters()));
        List<String> common = local.stream().filter(remote::contains).sorted().toList();
        Map<String, Integer> routeToId = new LinkedHashMap<>();
        Map<Integer, String> idToRoute = new LinkedHashMap<>();
        for (int i = 0; i < common.size(); i++) {
            int id = i + 1;
            routeToId.put(common.get(i), id);
            idToRoute.put(id, common.get(i));
        }
        State state = new State(routeToId, idToRoute);
        internals.attachment(State.class, state);
        AutoCloseable acceptor = internals.addDatagramAcceptor(new DatagramAcceptor() {
            @Override public boolean accepts(ReactiveSession ignored, SessionDatagram datagram) {
                // PRR1 belongs to this profile even when malformed or carrying an unknown route id.
                return DatagramRoutingCodec.isEnvelope(datagram.data());
            }
            @Override public CompletionStage<Void> handle(ReactiveSession ignored, SessionDatagram datagram) {
                DatagramRoutingCodec.Decoded decoded = DatagramRoutingCodec.decode(datagram.data());
                if (decoded == null) return CompletableFuture.completedFuture(null);
                String route = idToRoute.get(decoded.routeId());
                if (route == null) return CompletableFuture.completedFuture(null);
                List<DatagramRouteHandler> handlers = state.handlers.get(route);
                if (handlers == null || handlers.isEmpty()) return CompletableFuture.completedFuture(null);
                CompletionStage<Void> tail = CompletableFuture.completedFuture(null);
                RoutedDatagram routed = new RoutedDatagram(route, decoded.data());
                for (DatagramRouteHandler handler : List.copyOf(handlers)) {
                    tail = tail.thenCompose(v -> {
                        try {
                            CompletionStage<Void> next = handler.handle(routed);
                            return next == null ? CompletableFuture.completedFuture(null) : next.exceptionally(error -> null);
                        } catch (Throwable ignoredError) {
                            return CompletableFuture.completedFuture(null);
                        }
                    });
                }
                return tail;
            }
        });
        state.acceptor = acceptor;
        return CompletableFuture.completedFuture(() -> {
            try { acceptor.close(); } finally { internals.attachment(State.class, null); }
        });
    }

    static final class State {
        final Map<String, Integer> routeToId;
        final Map<Integer, String> idToRoute;
        final Map<String, CopyOnWriteArrayList<DatagramRouteHandler>> handlers = new java.util.concurrent.ConcurrentHashMap<>();
        final Map<String, LatestSend> latest = new java.util.concurrent.ConcurrentHashMap<>();
        volatile AutoCloseable acceptor;
        State(Map<String, Integer> routeToId, Map<Integer, String> idToRoute) {
            this.routeToId = Map.copyOf(routeToId);
            this.idToRoute = Map.copyOf(idToRoute);
        }
    }

    static final class LatestSend {
        byte[] data;
        final List<CompletableFuture<Void>> waiters = new ArrayList<>();
        boolean running;
        LatestSend(byte[] data) { this.data = data; }
    }
}
