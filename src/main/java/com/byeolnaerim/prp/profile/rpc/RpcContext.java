package com.byeolnaerim.prp.profile.rpc;

import com.byeolnaerim.prp.ReactiveSession;
import com.byeolnaerim.prp.ReactiveStream;

public record RpcContext(ReactiveSession session, ReactiveStream stream, String target) {}
