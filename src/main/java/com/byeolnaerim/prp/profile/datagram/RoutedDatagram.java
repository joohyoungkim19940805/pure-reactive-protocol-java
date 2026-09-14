package com.byeolnaerim.prp.profile.datagram;

import java.util.Objects;

public record RoutedDatagram(String route, byte[] data) {
    public RoutedDatagram {
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(data, "data");
        data = data.clone();
    }
    @Override public byte[] data() { return data.clone(); }
}
