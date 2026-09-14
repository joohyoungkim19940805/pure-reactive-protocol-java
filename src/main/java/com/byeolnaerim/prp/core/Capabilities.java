package com.byeolnaerim.prp.core;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.error.CapabilityMismatchException;
import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class Capabilities {
    public static final String PREFIX = "prp.capability/";
    public static final String DUPLEX = "prp.core.duplex-stream";
    public static final String TLV = "prp.core.tlv-attributes";
    public static final String SEQUENCE = "prp.core.sequence-u64";
    public static final String FRAGMENTATION = "prp.core.fragmentation";
    public static final String LIVENESS = "prp.core.liveness";
    public static final String LIMITS = "prp.core.limits";
    public static final int PEER_LIMITS_BYTES = 26;

    private Capabilities() {}

    public static List<CapabilityDescriptor> base(ProtocolLimits limits, LivenessOptions liveness) {
        return List.of(
            CapabilityDescriptor.version1(DUPLEX),
            CapabilityDescriptor.version1(TLV),
            CapabilityDescriptor.version1(SEQUENCE),
            CapabilityDescriptor.version1(FRAGMENTATION),
            new CapabilityDescriptor(LIVENESS, 1, 1, LivenessCodec.encodeParameters(liveness)),
            new CapabilityDescriptor(LIMITS, 1, 1, encodePeerLimits(limits))
        );
    }

    public static List<ProtocolAttribute> toAttributes(List<CapabilityDescriptor> capabilities, CapabilityPolicy policy) {
        List<ProtocolAttribute> output = new ArrayList<>();
        for (CapabilityDescriptor capability : capabilities) {
            ByteBuffer value = ByteBuffer.allocate(4 + capability.parameters().length).order(ByteOrder.BIG_ENDIAN);
            value.putShort((short) capability.minVersion());
            value.putShort((short) capability.maxVersion());
            value.put(capability.parameters());
            output.add(new ProtocolAttribute(PREFIX + capability.id(), value.array(), policy.require().contains(capability.id())));
        }
        return List.copyOf(output);
    }

    public static List<OfferedCapability> fromAttributes(List<ProtocolAttribute> attributes) {
        List<OfferedCapability> output = new ArrayList<>();
        Set<String> ids = new LinkedHashSet<>();
        for (ProtocolAttribute attribute : attributes) {
            if (!attribute.id().startsWith(PREFIX)) continue;
            String id = attribute.id().substring(PREFIX.length());
            if (id.isEmpty()) throw new ProtocolViolationException("Capability id must not be empty.");
            if (!ids.add(id)) throw new ProtocolViolationException("Capability " + id + " was offered more than once.");
            byte[] raw = attribute.value();
            if (raw.length < 4) throw new ProtocolViolationException("Capability " + id + " has an invalid descriptor.");
            ByteBuffer value = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN);
            int min = Short.toUnsignedInt(value.getShort());
            int max = Short.toUnsignedInt(value.getShort());
            if (max < min) throw new ProtocolViolationException("Capability " + id + " has an inverted version range.");
            byte[] parameters = new byte[value.remaining()];
            value.get(parameters);
            output.add(new OfferedCapability(new CapabilityDescriptor(id, min, max, parameters), attribute.required()));
        }
        return List.copyOf(output);
    }

    public static CapabilitySet negotiate(List<CapabilityDescriptor> local, List<OfferedCapability> remote, CapabilityPolicy policy) {
        Map<String, CapabilityDescriptor> localMap = new LinkedHashMap<>();
        for (CapabilityDescriptor capability : local) {
            if (localMap.putIfAbsent(capability.id(), capability) != null) throw new IllegalArgumentException("Duplicate local capability: " + capability.id());
        }
        Map<String, OfferedCapability> remoteMap = new LinkedHashMap<>();
        for (OfferedCapability offered : remote) remoteMap.put(offered.descriptor().id(), offered);
        for (OfferedCapability offered : remote) {
            if (offered.required() && !localMap.containsKey(offered.descriptor().id())) throw new CapabilityMismatchException("Required remote capability is unsupported: " + offered.descriptor().id());
        }
        for (String id : policy.require()) {
            if (!remoteMap.containsKey(id)) throw new CapabilityMismatchException("Required local capability is unavailable remotely: " + id);
        }
        List<NegotiatedCapability> negotiated = new ArrayList<>();
        for (Map.Entry<String, CapabilityDescriptor> entry : localMap.entrySet()) {
            OfferedCapability remoteOffer = remoteMap.get(entry.getKey());
            if (remoteOffer == null) continue;
            CapabilityDescriptor own = entry.getValue();
            CapabilityDescriptor theirs = remoteOffer.descriptor();
            int minimum = Math.max(own.minVersion(), theirs.minVersion());
            int maximum = Math.min(own.maxVersion(), theirs.maxVersion());
            if (minimum <= maximum) negotiated.add(new NegotiatedCapability(entry.getKey(), maximum, own, theirs));
            else if (remoteOffer.required() || policy.require().contains(entry.getKey())) throw new CapabilityMismatchException("No compatible version exists for required capability: " + entry.getKey());
        }
        return new CapabilitySet(negotiated);
    }

    public static byte[] encodePeerLimits(ProtocolLimits limits) {
        ByteBuffer buffer = ByteBuffer.allocate(PEER_LIMITS_BYTES).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(limits.maxFrameBytes());
        buffer.putInt(limits.maxAttributeBytes());
        buffer.putInt(limits.maxAttributes());
        buffer.putShort((short) limits.maxAttributeIdBytes());
        buffer.putInt(limits.maxAttributeValueBytes());
        buffer.putInt(limits.maxInboundStreams());
        buffer.putInt(limits.maxInboundItemBytes());
        return buffer.array();
    }

    public static PeerProtocolLimits decodePeerLimits(byte[] value) {
        if (value == null || value.length != PEER_LIMITS_BYTES) throw new ProtocolViolationException("prp.core.limits requires exactly 26 parameter bytes.");
        ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        int frame = checkedU32(buffer.getInt(), "maxFrameBytes");
        int attrs = checkedU32(buffer.getInt(), "maxAttributeBytes");
        int count = checkedU32(buffer.getInt(), "maxAttributes");
        int id = Short.toUnsignedInt(buffer.getShort());
        int attrValue = checkedU32(buffer.getInt(), "maxAttributeValueBytes");
        int streams = checkedU32(buffer.getInt(), "maxInboundStreams");
        int item = checkedU32(buffer.getInt(), "maxInboundItemBytes");
        if (frame < FrameCodec.FRAME_HEADER_BYTES || attrs >= frame || attrValue > attrs) throw new ProtocolViolationException("Malformed prp.core.limits parameters.");
        return new PeerProtocolLimits(frame, attrs, count, id, attrValue, streams, item);
    }

    private static int checkedU32(int raw, String name) {
        long value = Integer.toUnsignedLong(raw);
        if (value == 0 || value > Integer.MAX_VALUE) throw new ProtocolViolationException(name + " is outside the Java runtime range.");
        return (int) value;
    }

    public record OfferedCapability(CapabilityDescriptor descriptor, boolean required) {}
}
