package tech.kwik.core.cid;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.kwik.core.common.EncryptionLevel;
import tech.kwik.core.frame.DatagramFrame;
import tech.kwik.core.frame.QuicFrame;
import tech.kwik.core.impl.QuicConnectionImpl;
import tech.kwik.core.impl.Role;
import tech.kwik.core.impl.TransportParameters;
import tech.kwik.core.impl.Version;
import tech.kwik.core.log.NullLogger;
import tech.kwik.core.packet.PacketFilter;
import tech.kwik.core.send.SenderImpl;
import tech.kwik.core.stream.StreamManager;

class ZeroCidDatagramRegressionTest {
    @Test
    void aZeroLengthPeerCidHasLengthZeroWithoutChangingItsIdentity() {
        ConnectionIdManager manager = manager(new byte[0]);
        assertTrue(manager.peerUsesZeroLengthConnectionId());
        assertEquals(0, manager.getCurrentPeerConnectionIdLength());
        assertArrayEquals(new byte[0], manager.getInitialPeerConnectionId());
        assertArrayEquals(new byte[0], manager.getPeerConnectionId(null));
    }

    @Test
    void aNonzeroPeerCidKeepsItsLengthAndOriginalBytes() {
        byte[] cid = { 1, 2, 3, 4, 5, 6, 7, 8 };
        ConnectionIdManager manager = manager(cid);
        assertFalse(manager.peerUsesZeroLengthConnectionId());
        assertEquals(cid.length, manager.getCurrentPeerConnectionIdLength());
        assertArrayEquals(cid, manager.getInitialPeerConnectionId());
    }

    @Test
    void realDatagramSendingAcceptsTheLimitAndRejectsOversizeForBothCidLengths() {
        for (int cidLength : new int[] { 0, 8 }) {
            SenderImpl sender = mock(SenderImpl.class);
            DatagramConnection connection = mock(DatagramConnection.class, withSettings()
                .useConstructor(manager(new byte[cidLength]), sender, mock(StreamManager.class))
                .defaultAnswer(CALLS_REAL_METHODS));
            try {
                connection.negotiateDatagrams();
                assertTrue(connection.canSendDatagram());
                // 1200 packet bytes minus flags, CID, packet number, AEAD tag and DATAGRAM header.
                int limit = 1176 - cidLength;
                byte[] payload = new byte[limit];
                Arrays.fill(payload, (byte) 0xff);
                connection.sendDatagram(payload);
                assertEquals(limit, connection.maxDatagramDataSize());
                ArgumentCaptor<QuicFrame> sent = ArgumentCaptor.forClass(QuicFrame.class);
                verify(sender).sendWithPriority(sent.capture(), eq(EncryptionLevel.App), any());
                assertArrayEquals(payload, assertInstanceOf(DatagramFrame.class, sent.getValue()).getData());
                assertThrows(IllegalArgumentException.class, () -> connection.sendDatagram(new byte[limit + 1]));
                verify(sender, times(1)).sendWithPriority(any(), eq(EncryptionLevel.App), any());
            } finally {
                connection.releaseCallbacks();
            }
        }
    }

    private static ConnectionIdManager manager(byte[] peerCid) {
        return new ConnectionIdManager(peerCid, new byte[8], 8, 3, null,
            (error, reason) -> fail("Unexpected connection close: " + error + ": " + reason), new NullLogger());
    }

    /** Real constructor and datagram negotiation; only packet dispatch and unrelated collaborators are mocked. */
    abstract static class DatagramConnection extends QuicConnectionImpl {
        private final ConnectionIdManager manager;
        private final SenderImpl sender;
        private final StreamManager streams;

        DatagramConnection(ConnectionIdManager manager, SenderImpl sender, StreamManager streams) {
            super(Version.getDefault(), Role.Server, null, null, "zero-cid-regression", new NullLogger());
            this.manager = manager;
            this.sender = sender;
            this.streams = streams;
        }

        @Override public ConnectionIdManager getConnectionIdManager() { return manager; }
        @Override public int getMaxPacketSize() { return 1200; }
        @Override protected SenderImpl getSender() { return sender; }
        @Override protected StreamManager getStreamManager() { return streams; }
        @Override protected PacketFilter createProcessorChain() { return null; }

        void negotiateDatagrams() {
            enableDatagramExtension();
            TransportParameters peer = new TransportParameters();
            peer.setMaxDatagramFrameSize(1200);
            processCommonTransportParameters(peer);
        }

        void releaseCallbacks() { postTerminateHook(); }
    }
}
