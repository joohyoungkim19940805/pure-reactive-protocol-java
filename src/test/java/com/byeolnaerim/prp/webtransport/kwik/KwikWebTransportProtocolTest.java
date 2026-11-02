package com.byeolnaerim.prp.webtransport.kwik;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.InputStream;
import org.junit.jupiter.api.Test;

class KwikWebTransportProtocolTest {
    @Test
    void unknownAndGreaseFramesAreSkippedWithoutChangingKnownFrameBytes() throws Exception {
        byte[] known = frames(frame(1, new byte[] { (byte) 0x80, (byte) 0xff }),
            frame(0, new byte[] { 3, 4 }));
        byte[] wire = frames(frame(0x21, new byte[] { 0, 1, 2 }),
            frame(0x1f * 3 + 0x21, new byte[0]), frame(0x1234, new byte[] { 9 }), known);
        assertArrayEquals(known, filtered(wire).readAllBytes());
    }

    @Test
    void knownAndForbiddenHttp2FrameTypesRemainVisibleToTheHttp3Validator() throws Exception {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        for (long type : new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 13 })
            wire.write(frame(type, new byte[] { (byte) 0xff, (byte) 0x80 }));
        assertArrayEquals(wire.toByteArray(), filtered(wire.toByteArray()).readAllBytes());
    }

    @Test
    void zeroLengthFramesDoNotHideTheFollowingFrameOrChangeUnsignedPayloadBytes() throws Exception {
        byte[] known = frames(frame(1, new byte[0]), frame(0, new byte[] { (byte) 0x80, (byte) 0xff }));
        InputStream input = filtered(frames(frame(0x21, new byte[0]), known));
        assertEquals(0, input.read(new byte[1], 0, 0));
        for (byte value : known) assertEquals(Byte.toUnsignedInt(value), input.read());
        assertEquals(-1, input.read());
    }

    @Test
    void truncatedKnownAndUnknownFramePayloadsFailAtEof() throws Exception {
        for (long type : new long[] { 0, 0x21 }) {
            ByteArrayOutputStream wire = new ByteArrayOutputStream();
            KwikWebTransportProtocol.writeVarInt(wire, type);
            KwikWebTransportProtocol.writeVarInt(wire, 3);
            wire.write(7);
            assertThrows(EOFException.class, () -> filtered(wire.toByteArray()).readAllBytes());
        }
    }

    @Test
    void unknownFrameLengthsLargerThanAnArrayAreSkippedWithoutPayloadAllocation() throws Exception {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        KwikWebTransportProtocol.writeVarInt(wire, 0x21);
        KwikWebTransportProtocol.writeVarInt(wire, (long) Integer.MAX_VALUE + 1);
        assertThrows(EOFException.class, () -> filtered(wire.toByteArray()).read());
    }

    @Test
    void truncatedFrameHeadersFailRatherThanBecomingCleanEof() throws Exception {
        assertThrows(EOFException.class, () -> filtered(new byte[] { 0x40 }).read());
        assertThrows(EOFException.class, () -> filtered(new byte[] { 0 }).read());
    }

    @Test
    void connectCapsuleReadsDistinguishHighBitBytesFromEof() throws Exception {
        InputStream input = KwikWebTransportSession.unsigned(signedSingleByteInput(new byte[] { (byte) 0x80, (byte) 0xff }));
        assertEquals(0x80, input.read());
        assertEquals(0xff, input.read());
        assertEquals(-1, input.read());
    }

    @Test
    void connectCapsuleVarIntsAndBulkPayloadsRemainIntact() throws Exception {
        ByteArrayOutputStream wire = new ByteArrayOutputStream();
        KwikWebTransportProtocol.writeVarInt(wire, 1L << 30);
        KwikWebTransportProtocol.writeVarInt(wire, 2);
        wire.write(new byte[] { (byte) 0xff, (byte) 0x80 });
        InputStream input = KwikWebTransportSession.unsigned(signedSingleByteInput(wire.toByteArray()));
        assertEquals(1L << 30, KwikWebTransportProtocol.readVarInt(input).value());
        assertEquals(2, KwikWebTransportProtocol.readVarInt(input).value());
        assertArrayEquals(new byte[] { (byte) 0xff, (byte) 0x80 }, input.readNBytes(2));
        assertEquals(-1, input.read());
    }

    private static InputStream filtered(byte[] wire) {
        return new KwikHttp3ServerConnection.ExtensionFrameInput(new ByteArrayInputStream(wire));
    }

    private static InputStream signedSingleByteInput(byte[] data) {
        return new FilterInputStream(new ByteArrayInputStream(data)) {
            @Override public int read() throws java.io.IOException {
                int value = in.read();
                return value < 0 ? -1 : (byte) value;
            }
        };
    }

    private static byte[] frame(long type, byte[] payload) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        KwikWebTransportProtocol.writeVarInt(output, type);
        KwikWebTransportProtocol.writeVarInt(output, payload.length);
        output.write(payload);
        return output.toByteArray();
    }

    private static byte[] frames(byte[]... frames) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] frame : frames) output.write(frame);
        return output.toByteArray();
    }

    @Test
    void quicVarIntRoundTripsBoundaryValues() throws Exception {
        long[] values = {
            0L,
            63L,
            64L,
            16_383L,
            16_384L,
            (1L << 30) - 1,
            1L << 30,
            (1L << 62) - 1
        };

        for (long value : values) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            KwikWebTransportProtocol.writeVarInt(output, value);
            KwikWebTransportProtocol.VarInt decoded = KwikWebTransportProtocol.readVarInt(
                new ByteArrayInputStream(output.toByteArray())
            );
            assertEquals(value, decoded.value());
        }
    }

    @Test
    void webTransportApplicationErrorMappingMatchesDraft16Range() {
        assertEquals(0x52e4a40fa8dbL, KwikWebTransportProtocol.toHttp3Error(0L));
        assertEquals(0x52e5ac983162L, KwikWebTransportProtocol.toHttp3Error(0xffff_ffffL));
    }

    @Test
    void sessionIdMustBeClientInitiatedBidirectionalStreamId() {
        assertTrue(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(0L));
        assertTrue(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(4L));
        assertTrue(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(12L));
        assertFalse(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(1L));
        assertFalse(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(2L));
        assertFalse(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(3L));
        assertFalse(KwikWebTransportProtocol.isClientInitiatedBidirectionalStreamId(-1L));
    }
}
