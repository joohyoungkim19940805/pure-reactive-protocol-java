package com.byeolnaerim.prp.transport;

import java.util.List;

public record TransportDescription(String id, List<String> traits) {
    public TransportDescription {
        traits = traits == null ? List.of() : List.copyOf(traits);
    }
}
