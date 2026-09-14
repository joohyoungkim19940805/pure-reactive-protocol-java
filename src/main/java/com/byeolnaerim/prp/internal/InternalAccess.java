package com.byeolnaerim.prp.internal;

import com.byeolnaerim.prp.ReactiveSession;

public final class InternalAccess {
    private InternalAccess() {}

    public static SessionInternals of(ReactiveSession session) {
        if (!(session instanceof SessionInternals internals)) throw new IllegalArgumentException("The supplied session is not a Pure Reactive Protocol Java session.");
        return internals;
    }
}
