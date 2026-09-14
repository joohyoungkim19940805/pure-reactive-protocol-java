package com.byeolnaerim.prp.error;

public class CapabilityMismatchException extends PrpException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CapabilityMismatchException(String message) { super(message, "CAPABILITY_MISMATCH"); }
}
