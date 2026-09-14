package com.byeolnaerim.prp.core;

/** Limits for the optional native PRP best-effort datagram lane. */
public record DatagramOptions(int maxInboundBytes, int maxPending) {
    public static final DatagramOptions DEFAULT = new DatagramOptions(64 * 1024, 256);

    public DatagramOptions {
        if (maxInboundBytes <= 0 || maxInboundBytes > Integer.MAX_VALUE - DatagramCodec.HEADER_BYTES) throw new IllegalArgumentException("maxInboundBytes must be 1.." + (Integer.MAX_VALUE - DatagramCodec.HEADER_BYTES) + ".");
        if (maxPending <= 0) throw new IllegalArgumentException("maxPending must be positive.");
    }
}
