package com.byeolnaerim.prp.webtransport.kwik;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.cid.ConnectionIdManager;
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

class KwikDatagramAccessTest {
    @Test
    void unmodifiedKwikStillReproducesTheZeroCidFailureWhileTheAdapterPreservesTheCid() {
        try (Fixture fixture = new Fixture(new byte[0], 1200)) {
            fixture.connection.negotiateDatagrams(65535);
            assertTrue(fixture.manager.peerUsesZeroLengthConnectionId());
            assertThrows(NullPointerException.class, fixture.manager::getCurrentPeerConnectionIdLength);
            assertThrows(NullPointerException.class, fixture.connection::maxDatagramDataSize);
            assertThrows(NullPointerException.class, () -> fixture.connection.sendDatagram(new byte[1]));

            assertEquals(1176, KwikDatagramAccess.maxDatagramDataSize(fixture.connection));
            assertArrayEquals(new byte[0], fixture.manager.getInitialPeerConnectionId());
            assertArrayEquals(new byte[0], fixture.manager.getPeerConnectionId(null));
            assertTrue(fixture.connection.canSendDatagram());
            assertTrue(fixture.connection.canReceiveDatagram());
            verifyNoInteractions(fixture.sender);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void zeroCidSendingUsesTheActualNegotiatedLimitAndTheNativePriorityDatagramLane() throws Exception {
        for (int peerFrameLimit : new int[] { 32, 1000, 65535 }) {
            for (int packetLimit : new int[] { 1200, 1252 }) {
                try (Fixture fixture = new Fixture(new byte[0], packetLimit)) {
                    fixture.connection.negotiateDatagrams(peerFrameLimit);
                    int limit = Math.min(peerFrameLimit, packetLimit - 21) - 3;
                    assertEquals(limit, KwikDatagramAccess.maxDatagramDataSize(fixture.connection));
                    byte[] payload = new byte[limit];
                    Arrays.fill(payload, (byte) 0xff);
                    KwikDatagramAccess.sendDatagram(fixture.connection, payload);

                    ArgumentCaptor<QuicFrame> frame = ArgumentCaptor.forClass(QuicFrame.class);
                    ArgumentCaptor<Consumer<QuicFrame>> lost = ArgumentCaptor.forClass(Consumer.class);
                    verify(fixture.sender).sendWithPriority(frame.capture(), eq(EncryptionLevel.App), lost.capture());
                    DatagramFrame datagram = assertInstanceOf(DatagramFrame.class, frame.getValue());
                    assertArrayEquals(payload, datagram.getData());
                    assertTrue(datagram.getFrameLength() <= peerFrameLimit);
                    lost.getValue().accept(datagram);
                    verify(fixture.sender, times(1)).sendWithPriority(any(), eq(EncryptionLevel.App), any());
                    verify(fixture.connection, never()).sendDatagram(any());
                    verify(fixture.connection, never()).createStream(anyBoolean());

                    assertThrows(IllegalArgumentException.class,
                        () -> KwikDatagramAccess.sendDatagram(fixture.connection, new byte[limit + 1]));
                    verifyNoMoreInteractions(fixture.sender);
                }
            }
        }
    }

    @Test
    void tinyPeerFrameLimitsDoNotBecomeAnInventedMinimumPayloadCapacity() {
        for (int peerFrameLimit : new int[] { 1, 2 }) {
            try (Fixture fixture = new Fixture(new byte[0], 1200)) {
                fixture.connection.negotiateDatagrams(peerFrameLimit);
                assertEquals(peerFrameLimit - 3, KwikDatagramAccess.maxDatagramDataSize(fixture.connection));
                assertThrows(IllegalArgumentException.class,
                    () -> KwikDatagramAccess.sendDatagram(fixture.connection, new byte[0]));
                verifyNoInteractions(fixture.sender);
            }
        }
    }

    @Test
    void disabledAndReceiveOnlyConnectionsKeepTheirRealCapabilitiesAndCannotSend() {
        try (Fixture fixture = new Fixture(new byte[0], 1200)) {
            assertFalse(fixture.connection.canSendDatagram());
            assertThrows(IllegalStateException.class,
                () -> KwikDatagramAccess.sendDatagram(fixture.connection, new byte[1]));
            fixture.connection.negotiateDatagrams(0);
            assertFalse(fixture.connection.canSendDatagram());
            assertTrue(fixture.connection.canReceiveDatagram());
            assertThrows(IllegalStateException.class,
                () -> KwikDatagramAccess.sendDatagram(fixture.connection, null));
            verifyNoInteractions(fixture.sender);
        }
    }

    @Test
    void aNullPayloadCannotEnterTheNativeSender() {
        try (Fixture fixture = new Fixture(new byte[0], 1200)) {
            fixture.connection.negotiateDatagrams(1200);
            assertThrows(NullPointerException.class,
                () -> KwikDatagramAccess.sendDatagram(fixture.connection, null));
            verifyNoInteractions(fixture.sender);
        }
    }

    @Test
    void nonzeroCidConnectionsRetainTheOriginalKwikMethodsAndPeerIdentity() {
        byte[] cid = { 1, 2, 3, 4, 5, 6, 7, 8 };
        try (Fixture fixture = new Fixture(cid, 1200)) {
            fixture.connection.negotiateDatagrams(65535);
            assertFalse(fixture.manager.peerUsesZeroLengthConnectionId());
            assertEquals(1168, KwikDatagramAccess.maxDatagramDataSize(fixture.connection));
            verify(fixture.connection).maxDatagramDataSize();
            byte[] payload = new byte[1168];
            KwikDatagramAccess.sendDatagram(fixture.connection, payload);
            verify(fixture.connection).sendDatagram(payload);
            verify(fixture.sender).sendWithPriority(any(DatagramFrame.class), eq(EncryptionLevel.App), any());
            assertEquals(cid.length, fixture.manager.getCurrentPeerConnectionIdLength());
            assertArrayEquals(cid, fixture.manager.getInitialPeerConnectionId());
        }
    }

    @Test
    void otherQuicImplementationsAreDelegatedWithoutInspectingOrReplacingTheirTransport() {
        QuicConnection connection = mock(QuicConnection.class);
        when(connection.maxDatagramDataSize()).thenReturn(777);
        assertEquals(777, KwikDatagramAccess.maxDatagramDataSize(connection));
        byte[] payload = { 3, 5, 7 };
        KwikDatagramAccess.sendDatagram(connection, payload);
        verify(connection).maxDatagramDataSize();
        verify(connection).sendDatagram(payload);
        verifyNoMoreInteractions(connection);
    }

    private static final class Fixture implements AutoCloseable {
        final ConnectionIdManager manager;
        final SenderImpl sender = mock(SenderImpl.class);
        final DatagramConnection connection;

        Fixture(byte[] peerCid, int packetLimit) {
            manager = new ConnectionIdManager(peerCid, new byte[8], 8, 3, null,
                (error, reason) -> fail("Unexpected connection close: " + error + ": " + reason), new NullLogger());
            connection = mock(DatagramConnection.class, withSettings()
                .useConstructor(manager, sender, mock(StreamManager.class), packetLimit)
                .defaultAnswer(CALLS_REAL_METHODS));
        }

        @Override public void close() { connection.releaseCallbacks(); }
    }

    /** Real construction/negotiation; only packet dispatch and unrelated collaborators are mocked. */
    abstract static class DatagramConnection extends QuicConnectionImpl {
        private final ConnectionIdManager manager;
        private final SenderImpl sender;
        private final StreamManager streams;
        private final int packetLimit;

        DatagramConnection(ConnectionIdManager manager, SenderImpl sender, StreamManager streams, int packetLimit) {
            super(Version.getDefault(), Role.Server, null, null, "datagram-access-regression", new NullLogger());
            this.manager = manager;
            this.sender = sender;
            this.streams = streams;
            this.packetLimit = packetLimit;
        }

        @Override public ConnectionIdManager getConnectionIdManager() { return manager; }
        @Override public int getMaxPacketSize() { return packetLimit; }
        @Override protected SenderImpl getSender() { return sender; }
        @Override protected StreamManager getStreamManager() { return streams; }
        @Override protected PacketFilter createProcessorChain() { return null; }

        void negotiateDatagrams(long peerFrameLimit) {
            enableDatagramExtension();
            TransportParameters peer = new TransportParameters();
            peer.setMaxDatagramFrameSize(peerFrameLimit);
            processCommonTransportParameters(peer);
            // Exclude the real negotiation's ACK-delay/UDP-limit registration from dispatch assertions.
            clearInvocations(sender);
        }

        void releaseCallbacks() { postTerminateHook(); }
    }
}
