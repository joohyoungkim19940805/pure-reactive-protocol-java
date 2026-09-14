package com.byeolnaerim.prp;

import java.util.List;

public record StreamMessage(byte[] data, List<ProtocolAttribute> attributes) {
    public StreamMessage {
        data = data == null ? new byte[0] : data.clone();
        attributes = attributes == null ? List.of() : List.copyOf(attributes);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
