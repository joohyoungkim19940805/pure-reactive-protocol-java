package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/** Compact native PRP datagram envelope. Carrier datagram boundaries provide message framing. */
public final class DatagramCodec {
    public static final String CAPABILITY_ID = "prp.core.datagram";
    public static final int MAGIC = 0x50524431; // PRD1
    public static final int HEADER_BYTES = 4;

    private DatagramCodec() {}

    /** Capability parameter: unsigned 32-bit maximum application payload accepted by this endpoint. */
    public static byte[] encodeCapability(int maxInboundBytes) {
        if (maxInboundBytes <= 0) throw new IllegalArgumentException("maxInboundBytes must be positive.");
        return ByteBuffer.allocate(4).putInt(maxInboundBytes).array();
    }

    public static int decodeCapability(byte[] parameters) {
        if (parameters == null || parameters.length != 4) {
            throw new ProtocolViolationException("prp.core.datagram requires exactly 4 parameter bytes.");
        }
        long value = Integer.toUnsignedLong(ByteBuffer.wrap(parameters).getInt());
        if (value == 0 || value > Integer.MAX_VALUE) {
            throw new ProtocolViolationException("prp.core.datagram maxInboundBytes is outside the Java implementation range.");
        }
        return (int) value;
    }

    public static byte[] encode(byte[] payload, int maxFrameBytes) {
        if (payload == null) throw new NullPointerException("payload");
        long frameBytes = (long) HEADER_BYTES + payload.length;
        if (frameBytes > maxFrameBytes) {
            throw new IllegalArgumentException("PRP native datagram exceeds the carrier limit of " + maxFrameBytes + " bytes.");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) frameBytes);
        buffer.putInt(MAGIC);
        buffer.put(payload);
        return buffer.array();
    }

    public static byte[] decode(byte[] frame, int maxPayloadBytes) {
        if (frame == null || frame.length < HEADER_BYTES) throw new ProtocolViolationException("Truncated PRP native datagram.");
        if (ByteBuffer.wrap(frame, 0, HEADER_BYTES).getInt() != MAGIC) throw new ProtocolViolationException("Invalid PRP native datagram magic.");
        int payloadBytes = frame.length - HEADER_BYTES;
        if (payloadBytes > maxPayloadBytes) throw new ProtocolViolationException("PRP native datagram exceeds " + maxPayloadBytes + " application bytes.");
        return Arrays.copyOfRange(frame, HEADER_BYTES, frame.length);
    }
}
