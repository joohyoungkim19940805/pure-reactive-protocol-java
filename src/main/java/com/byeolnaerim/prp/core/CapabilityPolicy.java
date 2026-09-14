package com.byeolnaerim.prp.core;

import java.util.LinkedHashSet;
import java.util.Set;

public record CapabilityPolicy(Set<String> require) {
    public CapabilityPolicy {
        require = require == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(require));
    }

    public static CapabilityPolicy none() { return new CapabilityPolicy(Set.of()); }
}
