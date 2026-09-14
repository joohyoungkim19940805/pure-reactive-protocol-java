package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class LivenessCodec {
    public static final int PARAMETERS_BYTES = 8;
    private static final BigInteger MAX_U64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private LivenessCodec() {}

    public static byte[] encodeParameters(LivenessOptions options) {
        ByteBuffer buffer = ByteBuffer.allocate(PARAMETERS_BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt((int) options.interval().toMillis());
        buffer.putInt((int) options.timeout().toMillis());
        return buffer.array();
    }

    public static LivenessOptions decodeParameters(byte[] value) {
        if (value == null || value.length != PARAMETERS_BYTES) throw new ProtocolViolationException("prp.core.liveness requires exactly 8 parameter bytes.");
        ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        long interval = Integer.toUnsignedLong(buffer.getInt());
        long timeout = Integer.toUnsignedLong(buffer.getInt());
        if (interval == 0 || interval > 0x7fffffffL || timeout <= interval || timeout > 0x7fffffffL) throw new ProtocolViolationException("Malformed liveness parameters.");
        return new LivenessOptions(java.time.Duration.ofMillis(interval), java.time.Duration.ofMillis(timeout));
    }

    public static byte[] encodeProbe(BigInteger probe) {
        if (probe == null || probe.signum() <= 0 || probe.compareTo(MAX_U64) > 0) throw new IllegalArgumentException("Liveness probe id must fit unsigned 64 bits and be nonzero.");
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        byte[] raw = probe.toByteArray();
        int offset = raw.length > 8 ? raw.length - 8 : 0;
        int length = raw.length - offset;
        for (int index = length; index < 8; index++) buffer.put((byte) 0);
        buffer.put(raw, offset, length);
        return buffer.array();
    }

    public static BigInteger decodeProbe(byte[] value) {
        if (value == null || value.length != 8) throw new ProtocolViolationException("PRP PING/PONG payload must contain exactly one unsigned 64-bit probe id.");
        BigInteger probe = new BigInteger(1, value);
        if (probe.signum() == 0) throw new ProtocolViolationException("PRP PING/PONG probe id must be nonzero.");
        return probe;
    }
}
