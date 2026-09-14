package com.byeolnaerim.prp.error;

public class CapabilityMismatchException extends PrpException {
    public CapabilityMismatchException(String message) { super(message, "CAPABILITY_MISMATCH"); }
}
