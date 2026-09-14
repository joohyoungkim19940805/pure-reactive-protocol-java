package com.byeolnaerim.prp.profile.rpc.jackson;

import com.byeolnaerim.prp.profile.rpc.PayloadCodec;
import tools.jackson.databind.ObjectMapper;

public final class JacksonJsonPayloadCodec implements PayloadCodec {
    private final ObjectMapper objectMapper;

    public JacksonJsonPayloadCodec(ObjectMapper objectMapper) {
        this.objectMapper = java.util.Objects.requireNonNull(objectMapper);
    }

    @Override public String id() { return "application/json"; }

    @Override
    public byte[] encode(Object value) {
        if (value == null) return new byte[0];
        try { return objectMapper.writeValueAsBytes(value); }
        catch (Exception cause) { throw new IllegalArgumentException("RPC JSON payload is not JSON-serializable.", cause); }
    }

    @Override
    public <T> T decode(byte[] value, Class<T> type) {
        if (value.length == 0) return null;
        try { return objectMapper.readValue(value, type); }
        catch (Exception cause) { throw new IllegalArgumentException("RPC JSON payload is invalid for " + type.getName() + ".", cause); }
    }
}
