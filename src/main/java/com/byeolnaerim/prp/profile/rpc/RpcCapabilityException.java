package com.byeolnaerim.prp.profile.rpc;

import com.byeolnaerim.prp.error.CapabilityMismatchException;

public final class RpcCapabilityException extends CapabilityMismatchException {
    public RpcCapabilityException(String message) { super(message); }
}
