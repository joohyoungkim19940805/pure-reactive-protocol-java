package com.byeolnaerim.prp.error;

public final class LivenessTimeoutException extends PrpException {
    public LivenessTimeoutException(String message) { super(message, "LIVENESS_TIMEOUT"); }
}
