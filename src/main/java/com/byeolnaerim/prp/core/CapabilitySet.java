package com.byeolnaerim.prp.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CapabilitySet implements Iterable<NegotiatedCapability> {
    private final Map<String, NegotiatedCapability> values;

    public CapabilitySet(Collection<NegotiatedCapability> values) {
        Map<String, NegotiatedCapability> map = new LinkedHashMap<>();
        for (NegotiatedCapability value : values) map.put(value.id(), value);
        this.values = Map.copyOf(map);
    }

    public boolean has(String id) { return values.containsKey(id); }
    public NegotiatedCapability get(String id) { return values.get(id); }
    public List<NegotiatedCapability> toList() { return List.copyOf(values.values()); }
    @Override public Iterator<NegotiatedCapability> iterator() { return values.values().iterator(); }
}
