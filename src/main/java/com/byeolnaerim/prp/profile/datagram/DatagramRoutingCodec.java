package com.byeolnaerim.prp.profile.datagram;

import com.byeolnaerim.prp.error.CapabilityMismatchException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

final class DatagramRoutingCodec {
    static final int MAGIC = 0x50525231; // PRR1
    static final int HEADER_BYTES = 6;
    private static final int MAX_ROUTE_BYTES = 255;
    private static final int MAX_ROUTES = 0xffff;
    private static final Pattern ROUTE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._/-]*");

    private DatagramRoutingCodec() {}

    static List<String> normalize(Collection<String> routes) {
        if (routes == null) throw new NullPointerException("routes");
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String route : routes) {
            validate(route);
            if (!unique.add(route)) throw new IllegalArgumentException("Datagram routing profile routes must be unique: " + route);
        }
        if (unique.size() > MAX_ROUTES) throw new IllegalArgumentException("Datagram routing profile supports at most " + MAX_ROUTES + " routes.");
        return unique.stream().sorted().toList();
    }

    static byte[] encodeRoutes(Collection<String> routes) {
        List<String> normalized = normalize(routes);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write((normalized.size() >>> 8) & 0xff);
        out.write(normalized.size() & 0xff);
        for (String route : normalized) {
            byte[] bytes = route.getBytes(StandardCharsets.UTF_8);
            out.write(bytes.length);
            out.writeBytes(bytes);
        }
        return out.toByteArray();
    }

    static List<String> decodeRoutes(byte[] parameters) {
        if (parameters == null || parameters.length < 2) throw new CapabilityMismatchException("datagram-routing/1 is missing its route table.");
        int count = ((parameters[0] & 0xff) << 8) | (parameters[1] & 0xff);
        int offset = 2;
        List<String> routes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (offset >= parameters.length) throw new CapabilityMismatchException("datagram-routing/1 route table is truncated.");
            int length = parameters[offset++] & 0xff;
            if (length <= 0 || offset + length > parameters.length) throw new CapabilityMismatchException("datagram-routing/1 contains an invalid route length.");
            String route = new String(parameters, offset, length, StandardCharsets.UTF_8);
            offset += length;
            validate(route);
            routes.add(route);
        }
        if (offset != parameters.length) throw new CapabilityMismatchException("datagram-routing/1 route table has trailing bytes.");
        try { return normalize(routes); }
        catch (RuntimeException error) { throw new CapabilityMismatchException("datagram-routing/1 route table is invalid: " + error.getMessage()); }
    }

    static byte[] encode(int routeId, byte[] data) {
        if (routeId <= 0 || routeId > MAX_ROUTES) throw new IllegalArgumentException("Datagram route id is outside uint16 range.");
        if (data == null) throw new NullPointerException("data");
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + data.length);
        buffer.putInt(MAGIC);
        buffer.putShort((short) routeId);
        buffer.put(data);
        return buffer.array();
    }

    static boolean isEnvelope(byte[] data) {
        return data != null && data.length >= 4 && ByteBuffer.wrap(data, 0, 4).getInt() == MAGIC;
    }

    static Decoded decode(byte[] data) {
        if (data == null || data.length < HEADER_BYTES) return null;
        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.getInt() != MAGIC) return null;
        int routeId = Short.toUnsignedInt(buffer.getShort());
        if (routeId == 0) return null;
        return new Decoded(routeId, Arrays.copyOfRange(data, HEADER_BYTES, data.length));
    }

    private static void validate(String route) {
        if (route == null || !ROUTE.matcher(route).matches()) throw new IllegalArgumentException("Invalid datagram route: " + route);
        int bytes = route.getBytes(StandardCharsets.UTF_8).length;
        if (bytes <= 0 || bytes > MAX_ROUTE_BYTES) throw new IllegalArgumentException("Datagram route must encode to 1.." + MAX_ROUTE_BYTES + " bytes: " + route);
    }

    record Decoded(int routeId, byte[] data) {}
}
