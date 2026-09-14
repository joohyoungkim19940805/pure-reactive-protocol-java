package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public final class FrameCodec {
    public static final int PROTOCOL_MAGIC = 0x50525031;
    public static final int PROTOCOL_MAJOR = 1;
    public static final int PROTOCOL_MINOR = 0;
    public static final int FRAME_HEADER_BYTES = 36;
    public static final int DATA_FRAGMENTED_FLAG = 0x01;
    private static final int ATTRIBUTE_REQUIRED = 0x01;
    private static final BigInteger MAX_U64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    private FrameCodec() {}

    public static ProtocolFrame decode(byte[] input, ProtocolLimits limits) {
        if (input.length > limits.maxFrameBytes()) throw new ProtocolViolationException("Frame exceeds " + limits.maxFrameBytes() + " bytes.");
        if (input.length < FRAME_HEADER_BYTES) throw new ProtocolViolationException("Frame is shorter than the PRP/1 core header.");
        ByteBuffer buffer = ByteBuffer.wrap(input).order(ByteOrder.BIG_ENDIAN);
        if (buffer.getInt() != PROTOCOL_MAGIC) throw new ProtocolViolationException("Invalid PRP/1 magic.");
        int major = Byte.toUnsignedInt(buffer.get());
        int minor = Byte.toUnsignedInt(buffer.get());
        if (major != PROTOCOL_MAJOR) throw new ProtocolViolationException("Unsupported PRP major version " + major + ".");
        if (minor != PROTOCOL_MINOR) throw new ProtocolViolationException("Unsupported PRP minor version " + minor + ".");
        FrameKind kind = FrameKind.fromValue(Byte.toUnsignedInt(buffer.get()));
        int flags = Byte.toUnsignedInt(buffer.get());
        int headerBytes = Short.toUnsignedInt(buffer.getShort());
        if (headerBytes != FRAME_HEADER_BYTES) throw new ProtocolViolationException("PRP/1 core header length must be exactly 36 bytes.");
        BigInteger streamId = readUInt64(buffer);
        BigInteger sequence = readUInt64(buffer);
        if (sequence.signum() == 0) throw new ProtocolViolationException("PRP/1 peer sequence starts at 1.");
        long headerValue = Integer.toUnsignedLong(buffer.getInt());
        long attributeBytesLong = Integer.toUnsignedLong(buffer.getInt());
        if (buffer.getShort() != 0) throw new ProtocolViolationException("PRP/1 reserved header fields must be zero.");
        if (attributeBytesLong > limits.maxAttributeBytes() || attributeBytesLong > input.length - headerBytes) throw new ProtocolViolationException("Invalid attribute area length.");
        int attributeBytes = Math.toIntExact(attributeBytesLong);
        byte[] encodedAttributes = new byte[attributeBytes];
        buffer.get(encodedAttributes);
        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        ProtocolFrame frame = new ProtocolFrame(kind, streamId, sequence, flags, headerValue, decodeAttributes(encodedAttributes, limits), payload);
        validate(frame);
        return frame;
    }

    public static byte[] encode(ProtocolFrame frame, ProtocolLimits limits) {
        validateUInt64(frame.streamId(), "streamId");
        validateUInt64(frame.sequence(), "sequence");
        if (frame.sequence().signum() == 0) throw new IllegalArgumentException("sequence must be nonzero.");
        if (frame.flags() < 0 || frame.flags() > 0xff) throw new IllegalArgumentException("flags must fit u8.");
        if (frame.headerValue() < 0 || frame.headerValue() > 0xffffffffL) throw new IllegalArgumentException("header value must fit u32.");
        validate(frame);
        byte[] attributes = encodeAttributes(frame.attributes(), limits);
        int frameBytes = FRAME_HEADER_BYTES + attributes.length + frame.payload().length;
        if (frameBytes > limits.maxFrameBytes()) throw new IllegalArgumentException("Frame exceeds " + limits.maxFrameBytes() + " bytes.");
        ByteBuffer buffer = ByteBuffer.allocate(frameBytes).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(PROTOCOL_MAGIC);
        buffer.put((byte) PROTOCOL_MAJOR);
        buffer.put((byte) PROTOCOL_MINOR);
        buffer.put((byte) frame.kind().value());
        buffer.put((byte) frame.flags());
        buffer.putShort((short) FRAME_HEADER_BYTES);
        writeUInt64(buffer, frame.streamId());
        writeUInt64(buffer, frame.sequence());
        buffer.putInt((int) frame.headerValue());
        buffer.putInt(attributes.length);
        buffer.putShort((short) 0);
        buffer.put(attributes);
        buffer.put(frame.payload());
        return buffer.array();
    }

    public static int measureAttributeBytes(List<ProtocolAttribute> attributes, ProtocolLimits limits) {
        return encodeAttributes(attributes, limits).length;
    }

    public static void validate(ProtocolFrame frame) {
        boolean attrs = !frame.attributes().isEmpty();
        int payload = frame.payload().length;
        long value = frame.headerValue();
        switch (frame.kind()) {
            case HELLO, WELCOME -> {
                if (frame.streamId().signum() != 0 || frame.flags() != 0 || value != 0 || payload != 0) throw new ProtocolViolationException("Invalid HELLO/WELCOME frame shape.");
            }
            case CLOSE -> {
                if (frame.streamId().signum() != 0 || frame.flags() != 0 || value != 0 || attrs) throw new ProtocolViolationException("Invalid CLOSE frame shape.");
            }
            case PING, PONG -> {
                if (frame.streamId().signum() != 0 || frame.flags() != 0 || value != 0 || attrs || payload != 8) throw new ProtocolViolationException("Invalid PING/PONG frame shape.");
            }
            case OPEN -> {
                if (frame.streamId().signum() == 0 || frame.flags() != 0 || value != 0 || payload != 0) throw new ProtocolViolationException("Invalid OPEN frame shape.");
            }
            case DATA -> {
                if (frame.streamId().signum() == 0 || (frame.flags() & ~DATA_FRAGMENTED_FLAG) != 0) throw new ProtocolViolationException("Invalid DATA frame shape.");
                boolean fragmented = (frame.flags() & DATA_FRAGMENTED_FLAG) != 0;
                if (!fragmented && value != 0) throw new ProtocolViolationException("Non-fragmented DATA must not declare fragment length.");
                if (fragmented && (value <= 0 || payload >= value)) throw new ProtocolViolationException("Invalid fragmented DATA shape.");
            }
            case FRAGMENT -> {
                if (frame.streamId().signum() == 0 || frame.flags() != 0 || value != 0 || attrs || payload == 0) throw new ProtocolViolationException("Invalid FRAGMENT frame shape.");
            }
            case DEMAND -> {
                if (frame.streamId().signum() == 0 || frame.flags() != 0 || value <= 0 || attrs || payload != 0) throw new ProtocolViolationException("Invalid DEMAND frame shape.");
            }
            case COMPLETE -> {
                if (frame.streamId().signum() == 0 || frame.flags() != 0 || value != 0 || attrs || payload != 0) throw new ProtocolViolationException("Invalid COMPLETE frame shape.");
            }
            case CANCEL -> {
                if (frame.streamId().signum() == 0 || frame.flags() != 0 || value != 0 || attrs) throw new ProtocolViolationException("Invalid CANCEL frame shape.");
            }
            case ERROR -> {
                if (frame.flags() != 0 || value != 0) throw new ProtocolViolationException("Invalid ERROR frame shape.");
            }
            case SIGNAL -> {
                if (frame.streamId().signum() != 0 || frame.flags() != 0 || value != 0) throw new ProtocolViolationException("Invalid SIGNAL frame shape.");
            }
        }
    }

    private static byte[] encodeAttributes(List<ProtocolAttribute> attributes, ProtocolLimits limits) {
        if (attributes.size() > limits.maxAttributes()) throw new IllegalArgumentException("Frame has too many attributes.");
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (ProtocolAttribute attribute : attributes) {
            byte[] id = PrpText.utf8(attribute.id());
            byte[] value = attribute.value();
            if (id.length == 0 || id.length > limits.maxAttributeIdBytes()) throw new IllegalArgumentException("Invalid attribute id length.");
            if (value.length > limits.maxAttributeValueBytes()) throw new IllegalArgumentException("Attribute value is too large.");
            ByteBuffer header = ByteBuffer.allocate(7).order(ByteOrder.BIG_ENDIAN);
            header.put((byte) (attribute.required() ? ATTRIBUTE_REQUIRED : 0));
            header.putShort((short) id.length);
            header.putInt(value.length);
            output.writeBytes(header.array());
            output.writeBytes(id);
            output.writeBytes(value);
            if (output.size() > limits.maxAttributeBytes()) throw new IllegalArgumentException("Attribute area is too large.");
        }
        return output.toByteArray();
    }

    private static List<ProtocolAttribute> decodeAttributes(byte[] value, ProtocolLimits limits) {
        List<ProtocolAttribute> attributes = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        while (buffer.hasRemaining()) {
            if (attributes.size() >= limits.maxAttributes() || buffer.remaining() < 7) throw new ProtocolViolationException("Malformed attribute area.");
            int flags = Byte.toUnsignedInt(buffer.get());
            if ((flags & ~ATTRIBUTE_REQUIRED) != 0) throw new ProtocolViolationException("Undefined attribute flag bits.");
            int idLength = Short.toUnsignedInt(buffer.getShort());
            long valueLengthLong = Integer.toUnsignedLong(buffer.getInt());
            if (idLength == 0 || idLength > limits.maxAttributeIdBytes() || valueLengthLong > limits.maxAttributeValueBytes() || valueLengthLong > Integer.MAX_VALUE) throw new ProtocolViolationException("Invalid attribute length.");
            int valueLength = (int) valueLengthLong;
            if (buffer.remaining() < idLength + valueLength) throw new ProtocolViolationException("Truncated attribute value.");
            byte[] id = new byte[idLength];
            buffer.get(id);
            byte[] attributeValue = new byte[valueLength];
            buffer.get(attributeValue);
            String identifier = PrpText.strictText(id);
            if (identifier.isEmpty()) throw new ProtocolViolationException("Attribute id must not be empty.");
            attributes.add(new ProtocolAttribute(identifier, attributeValue, (flags & ATTRIBUTE_REQUIRED) != 0));
        }
        return List.copyOf(attributes);
    }

    private static BigInteger readUInt64(ByteBuffer buffer) {
        byte[] value = new byte[8];
        buffer.get(value);
        return new BigInteger(1, value);
    }

    private static void writeUInt64(ByteBuffer buffer, BigInteger value) {
        validateUInt64(value, "u64");
        byte[] raw = value.toByteArray();
        int offset = raw.length > 8 ? raw.length - 8 : 0;
        int length = raw.length - offset;
        for (int index = length; index < 8; index++) buffer.put((byte) 0);
        buffer.put(raw, offset, length);
    }

    private static void validateUInt64(BigInteger value, String name) {
        if (value == null || value.signum() < 0 || value.compareTo(MAX_U64) > 0) throw new IllegalArgumentException(name + " must fit unsigned 64 bits.");
    }
}
