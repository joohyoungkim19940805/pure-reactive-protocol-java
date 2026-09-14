package com.byeolnaerim.prp.error;

public final class LivenessTimeoutException extends PrpException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public LivenessTimeoutException(String message) { super(message, "LIVENESS_TIMEOUT"); }
}
