package com.byeolnaerim.prp.error;

public final class ConnectionLostException extends PrpException {
    public ConnectionLostException() { super("The physical PRP carrier was lost.", "CONNECTION_LOST"); }
    public ConnectionLostException(String message) { super(message, "CONNECTION_LOST"); }
    public ConnectionLostException(String message, Throwable cause) { super(message, "CONNECTION_LOST", cause); }
}
