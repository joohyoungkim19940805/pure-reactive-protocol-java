package com.byeolnaerim.prp.webtransport.kwik;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

class KwikWebTransportProtocolTest {
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
