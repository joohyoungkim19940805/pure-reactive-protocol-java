package com.byeolnaerim.prp.profile.datagram;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.error.CapabilityMismatchException;
import com.byeolnaerim.prp.error.PrpException;
import com.byeolnaerim.prp.internal.InternalAccess;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

public final class DatagramPeer {
    private final ReactiveSession session;
    private final DatagramRoutingProfile.State state;

    public DatagramPeer(ReactiveSession session) {
        this.session = java.util.Objects.requireNonNull(session);
        if (!session.supports(DatagramRoutingProfile.CAPABILITY_ID)) throw new CapabilityMismatchException("datagram-routing/1 is not negotiated for this session.");
        DatagramRoutingProfile.State next = InternalAccess.of(session).attachment(DatagramRoutingProfile.State.class);
        if (next == null) throw new CapabilityMismatchException("datagram-routing/1 profile is not attached to this session.");
        this.state = next;
    }

    public int maxPayloadBytes() { return Math.max(0, session.maxDatagramBytes() - DatagramRoutingCodec.HEADER_BYTES); }
    public List<String> routes() { return state.routeToId.keySet().stream().sorted().toList(); }
    public boolean supports(String route) { return state.routeToId.containsKey(route); }

    public AutoCloseable route(String route, DatagramRouteHandler handler) {
        requireRoute(route);
        CopyOnWriteArrayList<DatagramRouteHandler> handlers = state.handlers.computeIfAbsent(route, key -> new CopyOnWriteArrayList<>());
        handlers.add(java.util.Objects.requireNonNull(handler));
        return () -> {
            handlers.remove(handler);
            if (handlers.isEmpty()) state.handlers.remove(route, handlers);
        };
    }

    public CompletionStage<Void> send(String route, byte[] data) {
        Integer routeId = state.routeToId.get(route);
        if (routeId == null) return CompletableFuture.failedFuture(new PrpException("Datagram route was not negotiated: " + route, "DATAGRAM_ROUTE_UNAVAILABLE"));
        if (data == null) return CompletableFuture.failedFuture(new NullPointerException("data"));
        if (data.length > maxPayloadBytes()) return CompletableFuture.failedFuture(new IllegalArgumentException("Routed datagram exceeds " + maxPayloadBytes() + " application bytes for route " + route + "."));
        return session.sendDatagram(DatagramRoutingCodec.encode(routeId, data));
    }

    public CompletionStage<Void> sendLatest(String route, byte[] data) {
        try { requireRoute(route); }
        catch (Throwable error) { return CompletableFuture.failedFuture(error); }
        if (data == null) return CompletableFuture.failedFuture(new NullPointerException("data"));
        if (data.length > maxPayloadBytes()) return CompletableFuture.failedFuture(new IllegalArgumentException("Routed datagram exceeds " + maxPayloadBytes() + " application bytes for route " + route + "."));
        CompletableFuture<Void> waiter = new CompletableFuture<>();
        DatagramRoutingProfile.LatestSend entry;
        synchronized (state.latest) {
            entry = state.latest.computeIfAbsent(route, key -> new DatagramRoutingProfile.LatestSend(data.clone()));
            synchronized (entry) {
                entry.data = data.clone();
                entry.waiters.add(waiter);
                if (!entry.running) {
                    entry.running = true;
                    drainLatest(route, entry);
                }
            }
        }
        return waiter;
    }

    private void drainLatest(String route, DatagramRoutingProfile.LatestSend entry) {
        byte[] data;
        List<CompletableFuture<Void>> waiters;
        synchronized (entry) {
            if (entry.waiters.isEmpty()) {
                entry.running = false;
                state.latest.remove(route, entry);
                return;
            }
            data = entry.data.clone();
            waiters = List.copyOf(entry.waiters);
            entry.waiters.clear();
        }
        send(route, data).whenComplete((ignored, error) -> {
            for (CompletableFuture<Void> waiter : waiters) {
                if (error == null) waiter.complete(null); else waiter.completeExceptionally(error);
            }
            synchronized (entry) {
                if (entry.waiters.isEmpty()) {
                    entry.running = false;
                    state.latest.remove(route, entry);
                    return;
                }
            }
            drainLatest(route, entry);
        });
    }

    private void requireRoute(String route) {
        if (!supports(route)) throw new PrpException("Datagram route was not negotiated: " + route, "DATAGRAM_ROUTE_UNAVAILABLE");
    }
}
