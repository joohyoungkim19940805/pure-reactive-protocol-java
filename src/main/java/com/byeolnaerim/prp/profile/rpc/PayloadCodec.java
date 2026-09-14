package com.byeolnaerim.prp.profile.rpc;

public interface PayloadCodec {
    String id();
    byte[] encode(Object value);
    <T> T decode(byte[] value, Class<T> type);
}
