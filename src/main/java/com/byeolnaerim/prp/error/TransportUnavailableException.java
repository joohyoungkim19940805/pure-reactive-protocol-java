package com.byeolnaerim.prp.error;

public final class TransportUnavailableException extends PrpException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public TransportUnavailableException(String message) { super(message, "TRANSPORT_UNAVAILABLE"); }
    public TransportUnavailableException(String message, Throwable cause) { super(message, "TRANSPORT_UNAVAILABLE", cause); }
}
