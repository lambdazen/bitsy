package com.lambdazen.bitsy.store;

import junit.framework.TestCase;

public class SingleThreadedStringCanonicalizerTest extends TestCase {
    public void testCanonicalize() {
        IStringCanonicalizer canon = new SingleThreadedStringCanonicalizer();
        
        String abc1 = "a".concat("bc");
        String abc2 = "abc";
        
        assertEquals(abc1, abc2);
        assertNotSame(abc1, abc2);
        
        String c1 = canon.canonicalize(abc1);
        String c2 = canon.canonicalize(abc2);
        
        assertSame(c1, c2);
        assertSame(c1, canon.canonicalize("ab".concat("c")));
    }
}
