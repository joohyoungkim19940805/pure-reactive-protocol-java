package com.byeolnaerim.prp.core;

public record PeerProtocolLimits(
    int maxFrameBytes,
    int maxAttributeBytes,
    int maxAttributes,
    int maxAttributeIdBytes,
    int maxAttributeValueBytes,
    int maxInboundStreams,
    int maxInboundItemBytes
) {}
