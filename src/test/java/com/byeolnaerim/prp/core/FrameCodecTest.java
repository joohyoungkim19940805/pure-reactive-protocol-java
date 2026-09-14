package com.byeolnaerim.prp.core;

import static org.junit.jupiter.api.Assertions.*;

import com.byeolnaerim.prp.ProtocolAttribute;
import com.byeolnaerim.prp.error.ProtocolViolationException;
import java.math.BigInteger;
import java.util.List;
import org.junit.jupiter.api.Test;

class FrameCodecTest {
    @Test
    void roundTripFragmentedData() {
        ProtocolFrame frame = new ProtocolFrame(
            FrameKind.DATA,
            BigInteger.ONE,
            BigInteger.ONE,
            FrameCodec.DATA_FRAGMENTED_FLAG,
            1024,
            List.of(ProtocolAttribute.text("app.test", "value")),
            new byte[128]
        );
        ProtocolFrame decoded = FrameCodec.decode(FrameCodec.encode(frame, ProtocolLimits.DEFAULT), ProtocolLimits.DEFAULT);
        assertEquals(frame.kind(), decoded.kind());
        assertEquals(frame.streamId(), decoded.streamId());
        assertEquals(1024, decoded.fragmentLength());
        assertEquals("value", decoded.attributes().getFirst().text());
        assertEquals(128, decoded.payload().length);
    }

    @Test
    void rejectsReservedHeaderField() {
        ProtocolFrame frame = new ProtocolFrame(FrameKind.PING, BigInteger.ZERO, BigInteger.ONE, 0, 0, List.of(), LivenessCodec.encodeProbe(BigInteger.ONE));
        byte[] encoded = FrameCodec.encode(frame, ProtocolLimits.DEFAULT);
        encoded[34] = 1;
        assertThrows(ProtocolViolationException.class, () -> FrameCodec.decode(encoded, ProtocolLimits.DEFAULT));
    }

    @Test
    void rejectsUnfragmentedItemBeyondRuntimeLimitAtSessionLayerShapeStillValid() {
        ProtocolFrame frame = new ProtocolFrame(FrameKind.DATA, BigInteger.ONE, BigInteger.ONE, 0, 0, List.of(), new byte[128]);
        assertDoesNotThrow(() -> FrameCodec.encode(frame, ProtocolLimits.DEFAULT));
    }
}
