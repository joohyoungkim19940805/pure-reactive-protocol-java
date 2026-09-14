package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.ProtocolAttribute;
import java.math.BigInteger;
import java.util.List;

public record ProtocolFrame(
    FrameKind kind,
    BigInteger streamId,
    BigInteger sequence,
    int flags,
    long headerValue,
    List<ProtocolAttribute> attributes,
    byte[] payload
) {
    public ProtocolFrame {
        if (kind == null || streamId == null || sequence == null) throw new IllegalArgumentException("Frame kind, streamId and sequence are required.");
        attributes = attributes == null ? List.of() : List.copyOf(attributes);
        payload = payload == null ? new byte[0] : payload.clone();
    }

    @Override
    public byte[] payload() { return payload.clone(); }

    public long credit() { return kind == FrameKind.DEMAND ? headerValue : 0; }
    public long fragmentLength() { return kind == FrameKind.DATA && (flags & FrameCodec.DATA_FRAGMENTED_FLAG) != 0 ? headerValue : 0; }
}
