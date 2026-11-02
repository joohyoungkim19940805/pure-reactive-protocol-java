package com.byeolnaerim.prp.webtransport.kwik;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Objects;
import tech.kwik.core.QuicConnection;
import tech.kwik.core.common.EncryptionLevel;
import tech.kwik.core.frame.DatagramFrame;
import tech.kwik.core.impl.QuicConnectionImpl;
import tech.kwik.core.send.SenderImpl;

/** Keeps Kwik 0.12.1's valid zero-CID path out of its broken header-length lookup. */
final class KwikDatagramAccess {
    private static final int ZERO_CID_PACKET_OVERHEAD = 1 + 4 + 16; // flags, packet number, AEAD tag

    private KwikDatagramAccess() {}

    static int maxDatagramDataSize(QuicConnection connection) {
        QuicConnectionImpl zeroCid = zeroCidConnection(connection);
        if (zeroCid == null) return connection.maxDatagramDataSize();
        Access access = access();
        try {
            int peerFrameLimit = (int) access.frameLimit().invokeExact(zeroCid);
            return Math.min(peerFrameLimit, zeroCid.getMaxPacketSize() - ZERO_CID_PACKET_OVERHEAD)
                - DatagramFrame.getMaxMinimalFrameSize();
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new IllegalStateException("Could not read the negotiated Kwik datagram limit", error);
        }
    }

    static void sendDatagram(QuicConnection connection, byte[] data) {
        QuicConnectionImpl zeroCid = zeroCidConnection(connection);
        if (zeroCid == null) {
            connection.sendDatagram(data);
            return;
        }
        if (!zeroCid.canSendDatagram())
            throw new IllegalStateException("Datagram extension is not enabled for sending");
        Objects.requireNonNull(data, "data");
        if (data.length > maxDatagramDataSize(zeroCid))
            throw new IllegalArgumentException("Data too large for a single datagram frame");
        Access access = access();
        try {
            SenderImpl sender = (SenderImpl) access.sender().invokeExact(zeroCid);
            sender.sendWithPriority(new DatagramFrame(data), EncryptionLevel.App, lost -> {});
        } catch (RuntimeException | Error error) {
            throw error;
        } catch (Throwable error) {
            throw new IllegalStateException("Could not send a native Kwik datagram", error);
        }
    }

    private static QuicConnectionImpl zeroCidConnection(QuicConnection connection) {
        Objects.requireNonNull(connection, "connection");
        try {
            return connection instanceof QuicConnectionImpl kwik
                && kwik.getConnectionIdManager().peerUsesZeroLengthConnectionId() ? kwik : null;
        } catch (LinkageError error) {
            throw unavailable(error);
        }
    }

    private static Access access() {
        Access access = LazyAccess.VALUE;
        if (access.failure() != null) throw unavailable(access.failure());
        return access;
    }

    private static IllegalStateException unavailable(Throwable cause) {
        return new IllegalStateException("Kwik 0.12.1 zero-CID datagrams require access to its protected "
            + "datagram-limit and sender methods; use a supported classpath deployment or a corrected upstream Kwik release", cause);
    }

    private record Access(MethodHandle frameLimit, MethodHandle sender, Throwable failure) {}

    private static final class LazyAccess {
        static final Access VALUE = resolve();

        private static Access resolve() {
            try {
                Method frameLimit = QuicConnectionImpl.class.getDeclaredMethod("getMaxDatagramFrameSize");
                Method sender = QuicConnectionImpl.class.getDeclaredMethod("getSender");
                if (!frameLimit.trySetAccessible() || !sender.trySetAccessible())
                    throw new IllegalAccessException("Kwik protected datagram methods are not accessible");
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                return new Access(lookup.unreflect(frameLimit), lookup.unreflect(sender), null);
            } catch (ReflectiveOperationException | RuntimeException | LinkageError error) {
                // Cache an actionable failure, rather than poisoning the class with an initializer error.
                return new Access(null, null, error);
            }
        }
    }
}
