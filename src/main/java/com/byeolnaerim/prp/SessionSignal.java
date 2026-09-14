package com.byeolnaerim.prp;

import java.util.List;

public record SessionSignal(byte[] data, List<ProtocolAttribute> attributes) {
    public SessionSignal {
        data = data == null ? new byte[0] : data.clone();
        attributes = attributes == null ? List.of() : List.copyOf(attributes);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
