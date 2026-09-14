package com.byeolnaerim.prp;

import java.util.Objects;

/** One native PRP best-effort datagram. Delivery and ordering are not guaranteed. */
public record SessionDatagram(byte[] data) {
    public SessionDatagram {
        Objects.requireNonNull(data, "data");
        data = data.clone();
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
