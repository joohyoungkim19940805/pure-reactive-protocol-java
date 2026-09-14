package com.byeolnaerim.prp.spring;

import com.byeolnaerim.prp.core.PrpText;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PrpRouteRegistry {
    private final Map<Key, PrpRouteDescriptor> descriptors = new LinkedHashMap<>();
    private volatile boolean sealed;

    public synchronized void register(PrpRouteDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        if (sealed) throw new IllegalStateException("PRP route registry is already sealed.");
        validateRoute(descriptor.route());
        Key key = new Key(descriptor.route(), descriptor.interaction());
        PrpRouteDescriptor previous = descriptors.putIfAbsent(key, descriptor);
        if (previous != null) {
            throw new IllegalStateException(
                "Duplicate PRP route " + descriptor.route() + " for " + descriptor.interaction() + ": "
                    + previous.method().toGenericString() + " and " + descriptor.method().toGenericString()
            );
        }
    }

    public synchronized void seal() {
        if (sealed) return;
        Map<String, PrpRouteDescriptor> rpcRoutes = new LinkedHashMap<>();
        for (PrpRouteDescriptor descriptor : descriptors.values()) {
            if (descriptor.interaction() == PrpInteraction.DATAGRAM) continue;
            PrpRouteDescriptor previous = rpcRoutes.putIfAbsent(descriptor.route(), descriptor);
            if (previous != null && !previous.inputType().equals(descriptor.inputType())) {
                throw new IllegalStateException(
                    "PRP RPC route " + descriptor.route() + " uses different request payload types across interactions: "
                        + previous.interaction() + "=" + previous.inputType().getName() + ", "
                        + descriptor.interaction() + "=" + descriptor.inputType().getName() + ". "
                        + "rpc/1 shares one decoded input type per route."
                );
            }
        }
        sealed = true;
    }

    public boolean sealed() { return sealed; }

    public PrpRouteDescriptor require(String route, PrpInteraction interaction) {
        PrpRouteDescriptor descriptor = descriptors.get(new Key(route, interaction));
        if (descriptor == null) {
            throw new IllegalArgumentException("No PRP route is registered for " + route + " / " + interaction + ".");
        }
        return descriptor;
    }

    public PrpRouteDescriptor find(String route, PrpInteraction interaction) {
        return descriptors.get(new Key(route, interaction));
    }

    public List<PrpRouteDescriptor> routes() {
        return descriptors.values().stream()
            .sorted(Comparator.comparing(PrpRouteDescriptor::route).thenComparing(value -> value.interaction().name()))
            .toList();
    }

    public List<PrpRouteDescriptor> routes(PrpInteraction interaction) {
        List<PrpRouteDescriptor> output = new ArrayList<>();
        for (PrpRouteDescriptor descriptor : descriptors.values()) {
            if (descriptor.interaction() == interaction) output.add(descriptor);
        }
        output.sort(Comparator.comparing(PrpRouteDescriptor::route));
        return List.copyOf(output);
    }

    public List<String> datagramRoutes() {
        return routes(PrpInteraction.DATAGRAM).stream().map(PrpRouteDescriptor::route).toList();
    }

    private static void validateRoute(String route) {
        if (route == null || route.isBlank()) throw new IllegalArgumentException("PRP route must not be blank.");
        PrpText.utf8(route);
    }

    private record Key(String route, PrpInteraction interaction) {}
}
