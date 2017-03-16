package com.lambdazen.bitsy;

public class BitsyRetryException extends BitsyException  {
    private static final long serialVersionUID = 976641612846833462L;
    BitsyErrorCodes code;

    public BitsyRetryException(BitsyErrorCodes code) {
        super(code);
    }
    
    public BitsyRetryException(BitsyErrorCodes code, String s) {
        super(code, s);
    }
    
    public BitsyRetryException(BitsyErrorCodes code, String s, Throwable t) {
        super(code, s, t);
    }
}
