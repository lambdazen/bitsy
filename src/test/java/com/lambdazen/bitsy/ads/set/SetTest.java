package com.lambdazen.bitsy.ads.set;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

public class SetTest extends TestCase {
    private Random rand = new Random();
    
    public void testBasicExpandContract() {
        Object set = null; // empty set
        
        // Test remove on null
        set = CompactSet.remove(set, "foo");
        
        // Add foo
        set = CompactSet.add(set, "foo");        
        assertEquals(1, CompactSet.size(set));
        Object[] elements = CompactSet.getElements(set);
        assertEquals("foo", elements[0]);
        
        // Add foo again
        set = CompactSet.add(set, "foo");
        assertEquals(1, CompactSet.size(set));
        elements = CompactSet.getElements(set);
        assertEquals("foo", elements[0]);
        
        set = CompactSet.add(set, "bar");
        assertEquals(2, CompactSet.size(set));
        assertEquals("foo", CompactSet.getElements(set)[0]);
        assertEquals("bar", CompactSet.getElements(set)[1]);
        

        // Move down to 0
        set = CompactSet.remove(set, "foo"); // invalid key
        assertEquals(1, CompactSet.size(set));
        assertEquals("bar", CompactSet.getElements(set)[0]);

        set = CompactSet.remove(set, "foo"); // invalid key
        assertEquals(1, CompactSet.size(set));
        assertEquals("bar", CompactSet.getElements(set)[0]);
        
        set = CompactSet.remove(set, "bar");
        assertNull(set);
    }
    
    public void testRandomExpandContract() {
        randomTestIter(5000, 171);
        randomTestIter(100000, 17);
    }
    
    public void testClassTypes() {
        Object set = null; // empty set
        
        set = CompactSet.<Integer>add(set, 1);
        assertEquals(1, CompactSet.size(set));
        assertEquals(Integer.class, set.getClass());

        set = CompactSet.<Integer>add(set, 1);
        assertEquals(1, CompactSet.size(set));
        assertEquals(Integer.class, set.getClass());

        set = CompactSet.<Integer>add(set, 2);
        assertEquals(2, CompactSet.size(set));
        assertEquals(Set2.class, set.getClass());

        set = CompactSet.<Integer>add(set, 2);
        assertEquals(2, CompactSet.size(set));
        assertEquals(Set2.class, set.getClass());

        set = CompactSet.<Integer>add(set, 3);
        assertEquals(3, CompactSet.size(set));
        assertEquals(Set3.class, set.getClass());

        set = CompactSet.<Integer>add(set, 4);
        assertEquals(4, CompactSet.size(set));
        assertEquals(Set4.class, set.getClass());

        set = CompactSet.<Integer>add(set, 5);
        assertEquals(5, CompactSet.size(set));
        assertEquals(Set6.class, set.getClass());

        set = CompactSet.<Integer>add(set, 6);
        assertEquals(6, CompactSet.size(set));
        assertEquals(Set6.class, set.getClass());

        set = CompactSet.<Integer>add(set, 7);
        assertEquals(7, CompactSet.size(set));
        assertEquals(Set8.class, set.getClass());

        set = CompactSet.<Integer>add(set, 8);
        assertEquals(8, CompactSet.size(set));
        assertEquals(Set8.class, set.getClass());

        set = CompactSet.<Integer>add(set, 9);
        assertEquals(9, CompactSet.size(set));
        assertEquals(Set12.class, set.getClass());

        set = CompactSet.<Integer>add(set, 10);
        assertEquals(10, CompactSet.size(set));
        assertEquals(Set12.class, set.getClass());

        set = CompactSet.<Integer>add(set, 11);
        assertEquals(11, CompactSet.size(set));
        assertEquals(Set12.class, set.getClass());

        set = CompactSet.<Integer>add(set, 12);
        assertEquals(12, CompactSet.size(set));
        assertEquals(Set12.class, set.getClass());

        for (int i=13; i <= 24; i++) {
            set = CompactSet.<Integer>add(set, i);
            assertEquals(i, CompactSet.size(set));
            assertEquals(Set24.class, set.getClass());
        }
        
        set = CompactSet.<Integer>add(set, 25);
        assertEquals(25, CompactSet.size(set));
        assertEquals(SetMax.class, set.getClass());
        
        // Remove
        for (int i=25; i > 9; i--) {        
            set = CompactSet.<Integer>remove(set, i);
            assertEquals(i-1, CompactSet.size(set));
            
            if (i > 17) {
                assertEquals(SetMax.class, set.getClass());
            } else if (i > 13) {
                assertTrue((set instanceof Set24) || (set instanceof SetMax)); // Depends on hashcode 
            } else {
                assertEquals(Set12.class, set.getClass());
            }
        }
        
        set = CompactSet.<Integer>remove(set, 9);
        assertEquals(8, CompactSet.size(set));
        assertEquals(Set8.class, set.getClass());
        
        set = CompactSet.<Integer>remove(set, 8);
        assertEquals(7, CompactSet.size(set));
        assertEquals(Set8.class, set.getClass());
        
        set = CompactSet.<Integer>remove(set, 7);
        assertEquals(6, CompactSet.size(set));
        assertEquals(Set6.class, set.getClass());

        set = CompactSet.<Integer>remove(set, 6);
        assertEquals(5, CompactSet.size(set));
        assertEquals(Set6.class, set.getClass());

        set = CompactSet.<Integer>remove(set, 5);
        assertEquals(4, CompactSet.size(set));
        assertEquals(Set4.class, set.getClass());

        set = CompactSet.<Integer>remove(set, 4);
        assertEquals(3, CompactSet.size(set));
        assertEquals(Set3.class, set.getClass());

        set = CompactSet.<Integer>remove(set, 3);
        assertEquals(2, CompactSet.size(set));
        assertEquals(Set2.class, set.getClass());

        set = CompactSet.<Integer>remove(set, 2);
        assertEquals(1, CompactSet.size(set));
        assertEquals(Integer.class, set.getClass());

        set = CompactSet.<Integer>remove(set, 1);
        assertNull(set);
    }
    
    public void randomTestIter(int numIters, int numKeys) {
        for (boolean safe: new boolean[] {true, false}) {
            System.out.println("randomTestIter: " + numIters + ", " + numKeys + ", " + safe);
            
            java.util.Set<String> reference = new HashSet<String>();
            
            Object set = null;
    
            for (int i=0; i < numIters; i++) {          
                String key = "key " + (i % numKeys);
                
                if (rand.nextBoolean() || rand.nextBoolean()) {
                    // Prob 0.75 to set
                    if (safe) {
                        set = CompactSet.addSafe(set, key);
                    } else {
                        set = CompactSet.add(set, key);
                    }
    
                    reference.add(key);
                } else {
                    // Prob 0.25 to remove
                    set = CompactSet.remove(set, key);
                    
                    reference.remove(key);
                }
                
                compareAgainstSet(set, reference);
            }
        }
    }
    
    public void compareAgainstSet(Object set, java.util.Set<String> reference) {
        Object[] keys = CompactSet.getElements(set);
        
        List myKeys = Arrays.asList(keys);
        List refKeys = Arrays.asList(reference.toArray());
        
        Collections.sort(myKeys);
        Collections.sort(refKeys);
        
        assertEquals(reference.size(), keys.length);
        assertEquals(reference.size(), CompactSet.size(set));
        assertEquals(refKeys, myKeys);
    }
    
    public Object getRandomObject() {
        long time = System.currentTimeMillis();
        
        if (rand.nextBoolean()) {
            if (rand.nextBoolean()) {
                return new Long(time);
            } else {
                return "" + time;
            }
        } else {
            if (rand.nextBoolean()) {
                return new StringBuffer("" + time);
            } else {
                return null;
            }
        }
    }
}
