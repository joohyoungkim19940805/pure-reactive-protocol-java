package com.byeolnaerim.prp.error;

public final class ProtocolViolationException extends PrpException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public ProtocolViolationException(String message) { super(message, "PROTOCOL_VIOLATION"); }
    public ProtocolViolationException(String message, Throwable cause) { super(message, "PROTOCOL_VIOLATION", cause); }
}
