package com.byeolnaerim.prp.profile.rpc;

public final class BinaryPayloadCodec implements PayloadCodec {
    public static final BinaryPayloadCodec INSTANCE = new BinaryPayloadCodec();
    private BinaryPayloadCodec() {}
    @Override public String id() { return "application/octet-stream"; }
    @Override public byte[] encode(Object value) {
        if (!(value instanceof byte[] bytes)) throw new IllegalArgumentException("Binary RPC codec only accepts byte[].");
        return bytes.clone();
    }
    @Override public <T> T decode(byte[] value, Class<T> type) {
        if (type != byte[].class) throw new IllegalArgumentException("Binary RPC codec only decodes byte[].");
        return type.cast(value.clone());
    }
}
