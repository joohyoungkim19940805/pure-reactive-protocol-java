package com.byeolnaerim.prp.core;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.math.BigInteger;
import java.util.HexFormat;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CompactFrameCodecTest {
    private static final ProtocolLimits LIMITS = ProtocolLimits.DEFAULT;
    private static final HexFormat HEX = HexFormat.of();

    @SuppressWarnings("unchecked")
    @Test void consumesTheSharedTypescriptWireCorpus() throws Exception {
        try (var input = getClass().getResourceAsStream("/compatibility/compact-frames-v1.json")) {
            Map<String, Object> corpus = new tools.jackson.databind.ObjectMapper().readValue(input, Map.class);
            assertEquals(CompactFrameCodec.CAPABILITY_ID, corpus.get("capability"));
            for (var vector : (List<Map<String, Object>>) corpus.get("vectors")) {
                List<ProtocolAttribute> attributes = new ArrayList<>();
                for (var attribute : (List<Map<String, Object>>) vector.getOrDefault("attributes", List.of()))
                    attributes.add(new ProtocolAttribute((String) attribute.get("id"),
                        HEX.parseHex((String) attribute.get("valueHex")), Boolean.TRUE.equals(attribute.get("required"))));
                golden(new ProtocolFrame(FrameKind.valueOf((String) vector.get("kind")),
                    new BigInteger((String) vector.get("streamId")), new BigInteger((String) vector.get("sequence")),
                    ((Number) vector.getOrDefault("flags", 0)).intValue(),
                    ((Number) vector.getOrDefault("credit", vector.getOrDefault("fragmentLength", 0))).longValue(),
                    attributes, HEX.parseHex((String) vector.getOrDefault("payloadHex", ""))), (String) vector.get("wireHex"));
            }
            var nativeVector = (Map<String, String>) corpus.get("nativeDatagram");
            assertEquals(nativeVector.get("wireHex"), HEX.formatHex(
                DatagramCodec.encode(HEX.parseHex(nativeVector.get("payloadHex")), 1200, true)));
        }
    }

    @Test void sharesExactGoldenBytesWithTypescriptForDataCreditFragmentAndClose() {
        golden(new ProtocolFrame(FrameKind.DATA, BigInteger.ONE, BigInteger.TWO, 0, 0,
            List.of(), new byte[]{1, 2, 3}), "c1110102010203");
        golden(new ProtocolFrame(FrameKind.DEMAND, BigInteger.valueOf(300), BigInteger.valueOf(128), 0, 500,
            List.of(), new byte[0]), "c152ac028001f403");
        golden(new ProtocolFrame(FrameKind.DATA, BigInteger.ONE, BigInteger.valueOf(3), 1, 300,
            List.of(), new byte[]{1, 2}), "c1d10103ac020102");
        golden(new ProtocolFrame(FrameKind.CLOSE, BigInteger.ZERO, BigInteger.valueOf(4), 0, 0,
            List.of(), new byte[0]), "c1030004");
    }

    @Test void commonAttributeNamesUseCodesAndUnknownUnicodeNamesRoundtrip() {
        golden(new ProtocolFrame(FrameKind.OPEN, BigInteger.ONE, BigInteger.TWO, 0, 0, List.of(
            ProtocolAttribute.requiredText("prp.profile", "rpc/1"),
            ProtocolAttribute.requiredText("prp.rpc.target", "hunter.world.watch"),
            ProtocolAttribute.requiredText("prp.rpc.pattern", "server-stream")), new byte[0]),
            "c13001020303057270632f31051268756e7465722e776f726c642e7761746368070d7365727665722d73747265616d");
        golden(new ProtocolFrame(FrameKind.SIGNAL, BigInteger.ZERO, BigInteger.valueOf(129), 0, 0,
            List.of(ProtocolAttribute.of("☃", new byte[]{1, 2})), new byte[]{9}), "c136008101010003e2988302010209");
    }

    @Test void smallDataHeaderIsFourBytesInsteadOfThirtySixWithoutChangingPayload() {
        var frame = new ProtocolFrame(FrameKind.DATA, BigInteger.ONE, BigInteger.ONE, 0, 0, List.of(), new byte[5]);
        assertEquals(41, FrameCodec.encode(frame, LIMITS).length);
        assertEquals(9, CompactFrameCodec.encode(frame, LIMITS).length);
    }

    @Test void accountsForVarintGrowthAcrossOneThousandSmallAndMtuSizedMessages() {
        for (int payloadSize : List.of(5, 1100)) {
            long legacyBytes = 0, compactBytes = 0;
            for (int sequence = 1; sequence <= 1000; sequence++) {
                var frame = new ProtocolFrame(FrameKind.DATA, BigInteger.ONE, BigInteger.valueOf(sequence), 0, 0,
                    List.of(), new byte[payloadSize]);
                legacyBytes += FrameCodec.encode(frame, LIMITS).length;
                compactBytes += CompactFrameCodec.encode(frame, LIMITS).length;
            }
            assertEquals(1000L * (36 + payloadSize), legacyBytes);
            assertEquals(1000L * payloadSize + 4873, compactBytes);
            System.out.println("PRP_COMPACT_BYTES messages=1000 streamId=1 sequence=1..1000 payloadBytes=" + payloadSize
                + " legacyBytes=" + legacyBytes + " compactBytes=" + compactBytes);
        }
    }

    @Test void preservesTheWholeUnsigned64BitRange() {
        var maximum = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        var frame = new ProtocolFrame(FrameKind.DATA, maximum, maximum, 0, 0, List.of(), new byte[0]);
        var bytes = CompactFrameCodec.encode(frame, LIMITS);
        assertEquals(22, bytes.length);
        var result = CompactFrameCodec.decode(bytes, LIMITS);
        assertEquals(maximum, result.streamId()); assertEquals(maximum, result.sequence());
        assertThrows(IllegalArgumentException.class, () -> CompactFrameCodec.encode(
            new ProtocolFrame(FrameKind.DATA, maximum.add(BigInteger.ONE), maximum, 0, 0, List.of(), new byte[0]), LIMITS));
    }

    @Test void rejectsTruncatedOverlongNoncanonicalAndOverflowingIntegers() {
        for (String bytes : List.of("c11180", "c111018000", "c1110100",
                "c111ffffffffffffffffff0201", "c111808080808080808080800001", "c1520101ffffffff10"))
            assertThrows(ProtocolViolationException.class, () -> CompactFrameCodec.decode(HEX.parseHex(bytes), LIMITS), bytes);
    }

    @Test void rejectsNoncanonicalOptionalFieldsAndInvalidFrameShapes() {
        for (String bytes : List.of("c152010100", "c131010100", "c1810001", "c18500010000000000000000",
                "c1010001", "c1020001", "c1110001", "c11f0101"))
            assertThrows(ProtocolViolationException.class, () -> CompactFrameCodec.decode(HEX.parseHex(bytes), LIMITS), bytes);
    }

    @Test void dictionaryCompressionCannotBypassOriginalAttributeMemoryLimits() {
        var frame = new ProtocolFrame(FrameKind.OPEN, BigInteger.ONE, BigInteger.ONE, 0, 0,
            List.of(ProtocolAttribute.text("prp.profile", "rpc/1")), new byte[0]);
        var bytes = CompactFrameCodec.encode(frame, LIMITS);
        var limited = ProtocolLimits.builder().maxAttributeBytes(22).maxAttributeValueBytes(22).build();
        assertTrue(bytes.length < limited.maxAttributeBytes());
        assertThrows(ProtocolViolationException.class, () -> CompactFrameCodec.decode(bytes, limited));
        assertThrows(IllegalArgumentException.class, () -> CompactFrameCodec.encode(frame, limited));
    }

    @Test void rejectsMalformedAttributeCountsCodesLengthsAndUtf8() {
        for (String bytes : List.of("c1300101010c00", "c130010101000000", "c1300101010001ff00",
                "c13001010100036162", "c13001010102050102", "c1300101ffffffff0f"))
            assertThrows(ProtocolViolationException.class, () -> CompactFrameCodec.decode(HEX.parseHex(bytes), LIMITS), bytes);
    }

    @Test void physicalFrameLimitsStillApplyAndLegacyHandshakeIsUnchanged() {
        var frame = new ProtocolFrame(FrameKind.DATA, BigInteger.ONE, BigInteger.ONE, 0, 0, List.of(), new byte[40]);
        var limited = ProtocolLimits.builder().maxFrameBytes(64).maxAttributeBytes(32).maxAttributeValueBytes(32).build();
        var bytes = CompactFrameCodec.encode(frame, limited);
        assertEquals(44, bytes.length);
        var small = ProtocolLimits.builder(limited).maxFrameBytes(40).build();
        assertThrows(IllegalArgumentException.class, () -> CompactFrameCodec.encode(frame, small));
        assertThrows(ProtocolViolationException.class, () -> CompactFrameCodec.decode(bytes, small));
        var hello = new ProtocolFrame(FrameKind.HELLO, BigInteger.ZERO, BigInteger.ONE, 0, 0, List.of(), new byte[0]);
        assertEquals(36, FrameCodec.encode(hello, LIMITS).length);
        assertThrows(ProtocolViolationException.class, () -> CompactFrameCodec.encode(hello, LIMITS));
        assertThrows(ProtocolViolationException.class, () -> FrameCodec.decode(bytes, LIMITS));
    }

    @Test void nativeDatagramsUseOneByteOnlyWhenBothSidesNegotiateCompact() {
        var payload = new byte[]{1, 2, 3};
        byte[] compact = DatagramCodec.encode(payload, 4, true);
        assertEquals("d1010203", HEX.formatHex(compact));
        assertArrayEquals(payload, DatagramCodec.decode(compact, 3, true));
        assertThrows(IllegalArgumentException.class, () -> DatagramCodec.encode(payload, 3, true));
        assertThrows(ProtocolViolationException.class, () -> DatagramCodec.decode(compact, 2, true));
        assertThrows(ProtocolViolationException.class, () -> DatagramCodec.decode(compact, 3));
        assertThrows(ProtocolViolationException.class, () -> DatagramCodec.decode(DatagramCodec.encode(payload, 7), 3, true));
    }

    private static void golden(ProtocolFrame frame, String hex) {
        byte[] bytes = CompactFrameCodec.encode(frame, LIMITS);
        assertEquals(hex, HEX.formatHex(bytes));
        var decoded = CompactFrameCodec.decode(bytes, LIMITS);
        assertEquals(frame.kind(), decoded.kind()); assertEquals(frame.streamId(), decoded.streamId());
        assertEquals(frame.sequence(), decoded.sequence()); assertEquals(frame.flags(), decoded.flags());
        assertEquals(frame.headerValue(), decoded.headerValue()); assertArrayEquals(frame.payload(), decoded.payload());
        assertEquals(frame.attributes().size(), decoded.attributes().size());
        for (int i = 0; i < frame.attributes().size(); i++) {
            var original = frame.attributes().get(i); var actual = decoded.attributes().get(i);
            assertEquals(original.id(), actual.id()); assertEquals(original.required(), actual.required());
            assertArrayEquals(original.value(), actual.value());
        }
    }
}
