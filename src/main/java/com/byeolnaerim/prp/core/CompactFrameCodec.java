package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** Optional PRP/1 negotiated framing. HELLO/WELCOME always use the legacy codec. */
public final class CompactFrameCodec {
    public static final String CAPABILITY_ID = "prp.core.compact-frames";
    public static final int MAGIC = 0xc1;
    private static final int ATTRIBUTES = 0x20, HEADER_VALUE = 0x40, FRAGMENTED = 0x80;
    private static final List<String> ATTRIBUTE_IDS = List.of(
        "prp.profile", "prp.rpc.target", "prp.rpc.pattern", "prp.error.code", "prp.session.id");

    private CompactFrameCodec() {}

    public static boolean isCompact(byte[] bytes) {
        return bytes != null && bytes.length > 0 && (bytes[0] & 0xff) == MAGIC;
    }

    public static byte[] encode(ProtocolFrame frame, ProtocolLimits limits) {
        FrameCodec.validate(frame);
        rejectHandshake(frame.kind());
        requireUnsigned(frame.streamId(), 64, "streamId");
        requireUnsigned(frame.sequence(), 64, "sequence");
        if (frame.sequence().signum() == 0) throw new IllegalArgumentException("sequence must be nonzero.");
        if (frame.headerValue() < 0 || frame.headerValue() > 0xffffffffL)
            throw new IllegalArgumentException("header value must fit u32.");
        byte[] payload = frame.payload();
        byte[] attributes = encodeAttributes(frame.attributes(), limits);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(MAGIC);
        output.write(frame.kind().value() | (attributes.length == 0 ? 0 : ATTRIBUTES)
            | (frame.headerValue() == 0 ? 0 : HEADER_VALUE)
            | (frame.flags() == 0 ? 0 : FRAGMENTED));
        writeUnsigned(output, frame.streamId());
        writeUnsigned(output, frame.sequence());
        if (frame.headerValue() != 0) writeUnsigned(output, BigInteger.valueOf(frame.headerValue()));
        output.writeBytes(attributes);
        if ((long) output.size() + payload.length > limits.maxFrameBytes())
            throw new IllegalArgumentException("Frame exceeds " + limits.maxFrameBytes() + " bytes.");
        output.writeBytes(payload);
        return output.toByteArray();
    }

    public static ProtocolFrame decode(byte[] input, ProtocolLimits limits) {
        if (input == null || input.length < 4) throw invalid("Truncated compact PRP frame.");
        if (input.length > limits.maxFrameBytes()) throw invalid("Compact frame exceeds negotiated limit.");
        ByteBuffer buffer = ByteBuffer.wrap(input);
        if (Byte.toUnsignedInt(buffer.get()) != MAGIC) throw invalid("Invalid compact PRP magic.");
        int control = Byte.toUnsignedInt(buffer.get());
        FrameKind kind = FrameKind.fromValue(control & 0x1f);
        rejectHandshake(kind);
        BigInteger streamId = readUnsigned(buffer, 64), sequence = readUnsigned(buffer, 64);
        if (sequence.signum() == 0) throw invalid("PRP/1 peer sequence starts at 1.");
        long value = (control & HEADER_VALUE) == 0 ? 0 : readUnsigned(buffer, 32).longValue();
        if ((control & HEADER_VALUE) != 0 && value == 0) throw invalid("Redundant compact header value.");
        List<ProtocolAttribute> attributes = (control & ATTRIBUTES) == 0 ? List.of() : decodeAttributes(buffer, limits);
        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        ProtocolFrame frame = new ProtocolFrame(kind, streamId, sequence,
            (control & FRAGMENTED) == 0 ? 0 : FrameCodec.DATA_FRAGMENTED_FLAG, value, attributes, payload);
        FrameCodec.validate(frame);
        return frame;
    }

    /** Includes count and packed entries. Legacy logical attribute limits still apply. */
    public static int measureAttributeBytes(List<ProtocolAttribute> attributes, ProtocolLimits limits) {
        return encodeAttributes(attributes, limits).length;
    }

    private static byte[] encodeAttributes(List<ProtocolAttribute> attributes, ProtocolLimits limits) {
        FrameCodec.measureAttributeBytes(attributes, limits);
        if (attributes.isEmpty()) return new byte[0];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeUnsigned(output, BigInteger.valueOf(attributes.size()));
        for (ProtocolAttribute attribute : attributes) {
            int code = ATTRIBUTE_IDS.indexOf(attribute.id()) + 1;
            writeUnsigned(output, BigInteger.valueOf((code << 1) | (attribute.required() ? 1 : 0)));
            if (code == 0) {
                byte[] identifier = PrpText.utf8(attribute.id());
                writeUnsigned(output, BigInteger.valueOf(identifier.length));
                output.writeBytes(identifier);
            }
            byte[] value = attribute.value();
            writeUnsigned(output, BigInteger.valueOf(value.length));
            output.writeBytes(value);
        }
        return output.toByteArray();
    }

    private static List<ProtocolAttribute> decodeAttributes(ByteBuffer input, ProtocolLimits limits) {
        long count = readUnsigned(input, 32).longValue();
        if (count == 0 || count > limits.maxAttributes()) throw invalid("Invalid compact attribute count.");
        List<ProtocolAttribute> attributes = new ArrayList<>((int) Math.min(count, 256));
        long logicalBytes = 0;
        for (int i = 0; i < count; i++) {
            long tag = readUnsigned(input, 32).longValue(), code = tag >>> 1;
            String id;
            int idBytes;
            if (code == 0) {
                int length = checkedLength(readUnsigned(input, 32), limits.maxAttributeIdBytes(), input.remaining());
                if (length == 0) throw invalid("Attribute id must not be empty.");
                byte[] identifier = new byte[length]; input.get(identifier);
                id = PrpText.strictText(identifier); idBytes = length;
            } else {
                if (code > ATTRIBUTE_IDS.size()) throw invalid("Unknown compact attribute code.");
                id = ATTRIBUTE_IDS.get((int) code - 1); idBytes = PrpText.utf8(id).length;
                if (idBytes > limits.maxAttributeIdBytes()) throw invalid("Attribute id exceeds negotiated limit.");
            }
            int length = checkedLength(readUnsigned(input, 32), limits.maxAttributeValueBytes(), input.remaining());
            logicalBytes += 7L + idBytes + length;
            if (logicalBytes > limits.maxAttributeBytes()) throw invalid("Logical attribute area exceeds negotiated limit.");
            byte[] value = new byte[length]; input.get(value);
            attributes.add(new ProtocolAttribute(id, value, (tag & 1) != 0));
        }
        return List.copyOf(attributes);
    }

    private static int checkedLength(BigInteger value, int limit, int remaining) {
        if (value.compareTo(BigInteger.valueOf(Math.min(limit, remaining))) > 0)
            throw invalid("Compact attribute length exceeds available bytes or negotiated limit.");
        return value.intValue();
    }

    private static void rejectHandshake(FrameKind kind) {
        if (kind == FrameKind.HELLO || kind == FrameKind.WELCOME)
            throw invalid("HELLO/WELCOME must use legacy PRP/1 bootstrap framing.");
    }

    private static void requireUnsigned(BigInteger value, int bits, String name) {
        if (value == null || value.signum() < 0 || value.bitLength() > bits)
            throw new IllegalArgumentException(name + " must fit unsigned " + bits + " bits.");
    }

    private static void writeUnsigned(ByteArrayOutputStream output, BigInteger value) {
        do {
            int next = value.and(BigInteger.valueOf(0x7f)).intValue();
            value = value.shiftRight(7);
            output.write(next | (value.signum() == 0 ? 0 : 0x80));
        } while (value.signum() != 0);
    }

    private static BigInteger readUnsigned(ByteBuffer input, int bits) {
        BigInteger value = BigInteger.ZERO;
        for (int i = 0; i < (bits + 6) / 7; i++) {
            if (!input.hasRemaining()) throw invalid("Truncated compact unsigned integer.");
            int next = Byte.toUnsignedInt(input.get()), payload = next & 0x7f;
            value = value.or(BigInteger.valueOf(payload).shiftLeft(i * 7));
            if (value.bitLength() > bits) throw invalid("Compact unsigned integer overflow.");
            if ((next & 0x80) == 0) {
                if (i > 0 && payload == 0) throw invalid("Noncanonical compact unsigned integer.");
                return value;
            }
        }
        throw invalid("Compact unsigned integer is too long.");
    }

    private static ProtocolViolationException invalid(String reason) { return new ProtocolViolationException(reason); }
}
