package com.byeolnaerim.prp.error;

public class PrpException extends RuntimeException {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final String code;

    public PrpException(String message, String code) {
        super(message);
        this.code = code;
    }

    public PrpException(String message, String code, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
