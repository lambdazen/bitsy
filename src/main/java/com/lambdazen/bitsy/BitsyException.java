package com.lambdazen.bitsy;

public class BitsyException extends RuntimeException {
    private static final long serialVersionUID = -5310572247323732287L;
    BitsyErrorCodes code;

    public BitsyException(BitsyErrorCodes code) {
        super(code.toString());

        this.code = code;
    }

    public BitsyException(BitsyErrorCodes code, String s) {
        super(code.toString() + ". " + s);

        this.code = code;
    }

    public BitsyException(BitsyErrorCodes code, String s, Throwable t) {
        super(code.toString() + ". " + s, t);

        this.code = code;
    }

    public BitsyErrorCodes getErrorCode() {
        return code;
    }
}
