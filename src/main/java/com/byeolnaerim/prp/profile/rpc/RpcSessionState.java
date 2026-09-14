package com.byeolnaerim.prp.profile.rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class RpcSessionState {
    final PayloadCodec codec;
    final Map<String, RpcHandlers<?, ?>> handlers = new ConcurrentHashMap<>();
    AutoCloseable acceptorDisposer;

    RpcSessionState(PayloadCodec codec) { this.codec = codec; }
}
