package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.UUID;
import junit.framework.TestCase;

public class EndpointTest extends TestCase {
    public EndpointTest() {
    }
    
    public void testCompare() throws Exception {
        UUID vid = UUID.randomUUID();
        UUID eid = UUID.randomUUID();
        Endpoint e1;
        Endpoint e2;
        
        e1 = new Endpoint(null, null);
        e1.setMarker();
        e2 = new Endpoint(null, eid);
        
        assertTrue(e1.isMatch(e2));
        assertEquals(-1, e1.compareTo(e2));
        assertEquals(1, e2.compareTo(e1));
     
        e2 = new Endpoint("something", eid);
        assertTrue(e1.isMatch(e2));
        assertEquals(-1, e1.compareTo(e2));
        assertEquals(1, e2.compareTo(e1));
        assertFalse(e1.equals(e2));

//        e2 = new Endpoint(eid, null, eid);
//        assertFalse(e1.isMatch(e2));
//        assertEquals(vid.compareTo(eid), e1.compareTo(e2));
//        assertEquals(-vid.compareTo(eid), e2.compareTo(e1));
//        assertFalse(e1.equals(e2));
        
        e1 = new Endpoint("foo", null);
        e1.setMarker();
        
        e2 = new Endpoint("foo", eid);
        
        assertTrue(e1.isMatch(e2));
        assertEquals(-1, e1.compareTo(e2));
        assertEquals(1, e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
        e2 = new Endpoint("bar", eid);
        assertFalse(e1.isMatch(e2));
        assertEquals("foo".compareTo("bar"), e1.compareTo(e2));
        assertEquals("bar".compareTo("foo"), e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
        e1 = new Endpoint("foo", eid);
        e1.setMarker();
        
        e2 = new Endpoint("foo", eid);
        assertTrue(e1.isMatch(e2));
        assertEquals(-1, e1.compareTo(e2));
        assertEquals(1, e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
        e2 = new Endpoint("bar", eid);
        assertFalse(e1.isMatch(e2));
        assertEquals("foo".compareTo("bar"), e1.compareTo(e2));
        assertEquals("bar".compareTo("foo"), e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
        e2 = new Endpoint("foo", vid);
        assertFalse(e1.isMatch(e2));
        assertEquals(eid.compareTo(vid), e1.compareTo(e2));
        assertEquals(-eid.compareTo(vid), e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
        e1 = new Endpoint("foo", eid);
        
        e2 = new Endpoint("foo", eid);
        assertEquals(0, e1.compareTo(e2));
        assertEquals(e1, e2);
        
        e2 = new Endpoint("bar", eid);
        assertEquals("foo".compareTo("bar"), e1.compareTo(e2));
        assertEquals("bar".compareTo("foo"), e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
        e2 = new Endpoint("foo", vid);
        assertEquals(eid.compareTo(vid), e1.compareTo(e2));
        assertEquals(vid.compareTo(eid), e2.compareTo(e1));
        assertFalse(e1.equals(e2));
        
//        e2 = new Endpoint(eid, "foo", vid);
//        assertEquals(vid.compareTo(eid), e1.compareTo(e2));
//        assertEquals(eid.compareTo(vid), e2.compareTo(e1));
//        assertFalse(e1.equals(e2));
    }
}
