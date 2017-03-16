package com.lambdazen.bitsy.store;

import java.util.HashMap;
import java.util.Map;

/* This class is not thread-safe */
public class SingleThreadedStringCanonicalizer implements IStringCanonicalizer {
    Map<String, String> canonicalStrings;
    
    public SingleThreadedStringCanonicalizer() {
        canonicalStrings = new HashMap<String, String>();
    }
    
    public String canonicalize(String str) {
        String canonicalString = canonicalStrings.get(str);
        if (canonicalString != null) {
            return canonicalString;
        } else {
            canonicalStrings.put(str, str);

            return str;
        }
    }
}
