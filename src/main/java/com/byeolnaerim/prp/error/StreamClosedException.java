package com.byeolnaerim.prp.error;

public final class StreamClosedException extends PrpException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public StreamClosedException() { super("The PRP stream is closed.", "STREAM_CLOSED"); }
    public StreamClosedException(String message) { super(message, "STREAM_CLOSED"); }
}
