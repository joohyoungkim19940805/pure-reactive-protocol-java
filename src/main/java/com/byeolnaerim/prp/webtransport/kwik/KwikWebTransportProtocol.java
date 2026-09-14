package com.byeolnaerim.prp.webtransport.kwik;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

final class KwikWebTransportProtocol {
    static final String CURRENT_PROTOCOL = "webtransport-h3";
    static final String LEGACY_DRAFT07_PROTOCOL = "webtransport";

    static final long SETTINGS_H3_DATAGRAM = 0x33L;
    static final long SETTINGS_WT_ENABLED = 0x2c7cf000L;
    static final long SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07 = 0xc671706aL;
    static final long SETTINGS_ENABLE_WEBTRANSPORT_DRAFT02 = 0x2b603742L;

    // draft-ietf-webtrans-http3-16, draft-ietf-webtrans-http3-07, and draft-ietf-webtrans-http3-02
    static final long BIDIRECTIONAL_STREAM_SIGNAL = 0x41L;
    static final long UNIDIRECTIONAL_STREAM_TYPE = 0x54L;

    static final long H3_FRAME_ERROR = 0x0106L;
    static final long H3_ID_ERROR = 0x0108L;

    static final long WT_BUFFERED_STREAM_REJECTED = 0x3994bd84L;
    static final long WT_SESSION_GONE = 0x170d7b68L;
    static final long WT_REQUIREMENTS_NOT_MET = 0x212c0d48L;

    // WebTransport HTTP/3 application error-code mapping.
    private static final long FIRST_HTTP3_ERROR = 0x52e4a40fa8dbL;
    private static final long ERROR_INTERVAL = 0x1eL;

    private KwikWebTransportProtocol() {}

    static boolean isClientInitiatedBidirectionalStreamId(long streamId) {
        return streamId >= 0 && (streamId & 0x03L) == 0L;
    }

    static long toHttp3Error(long applicationErrorCode) {
        if (applicationErrorCode < 0 || applicationErrorCode > 0xffff_ffffL) {
            throw new IllegalArgumentException("WebTransport application error code must fit in an unsigned 32-bit integer.");
        }
        return FIRST_HTTP3_ERROR + applicationErrorCode + (applicationErrorCode / ERROR_INTERVAL);
    }

    static VarInt readVarInt(InputStream input) throws IOException {
        int first = input.read();
        if (first < 0) throw new EOFException("Unexpected end of QUIC variable-length integer.");
        int length = 1 << ((first >>> 6) & 0x03);
        long value = first & 0x3f;
        byte[] encoded = new byte[length];
        encoded[0] = (byte) first;
        for (int index = 1; index < length; index++) {
            int next = input.read();
            if (next < 0) throw new EOFException("Unexpected end of QUIC variable-length integer.");
            encoded[index] = (byte) next;
            value = (value << 8) | (next & 0xffL);
        }
        return new VarInt(value, encoded);
    }

    static void writeVarInt(OutputStream output, long value) throws IOException {
        if (value < 0 || value >= (1L << 62)) {
            throw new IllegalArgumentException("QUIC variable-length integer is out of range: " + value);
        }
        if (value < (1L << 6)) {
            output.write((int) value);
            return;
        }
        if (value < (1L << 14)) {
            output.write((int) ((value >>> 8) | 0x40));
            output.write((int) value);
            return;
        }
        if (value < (1L << 30)) {
            output.write((int) ((value >>> 24) | 0x80));
            output.write((int) (value >>> 16));
            output.write((int) (value >>> 8));
            output.write((int) value);
            return;
        }
        output.write((int) ((value >>> 56) | 0xc0));
        output.write((int) (value >>> 48));
        output.write((int) (value >>> 40));
        output.write((int) (value >>> 32));
        output.write((int) (value >>> 24));
        output.write((int) (value >>> 16));
        output.write((int) (value >>> 8));
        output.write((int) value);
    }

    static int varIntLength(long value) {
        if (value < 0 || value >= (1L << 62)) throw new IllegalArgumentException("QUIC variable-length integer is out of range: " + value);
        if (value < (1L << 6)) return 1;
        if (value < (1L << 14)) return 2;
        if (value < (1L << 30)) return 4;
        return 8;
    }

    record VarInt(long value, byte[] encoded) {}
}
