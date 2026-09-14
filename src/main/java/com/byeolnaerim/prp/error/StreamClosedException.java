package com.byeolnaerim.prp.error;

public final class StreamClosedException extends PrpException {
    public StreamClosedException() { super("The PRP stream is closed.", "STREAM_CLOSED"); }
    public StreamClosedException(String message) { super(message, "STREAM_CLOSED"); }
}
