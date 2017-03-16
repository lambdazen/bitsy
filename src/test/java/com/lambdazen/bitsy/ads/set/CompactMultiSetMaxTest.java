package com.lambdazen.bitsy.ads.set;

import java.util.Arrays;

import junit.framework.TestCase;

public class CompactMultiSetMaxTest extends TestCase {
    public CompactMultiSetMaxTest() {
        
    }
    
    public void testSimpleUse() {
        CompactMultiSetMax<String, String> test = new CompactMultiSetMax<String, String>(4, false);
        
        ClassifierGetter<String, String> c = new ClassifierGetter<String, String>() {
            @Override
            public String getClassifier(String obj) {
                return obj.substring(0, 3);
            }
        };

        test = test.add("fooBar", c); // classifier is foo
        test = test.add("fooBaz", c); // classifier is foo
        
        test = test.add("barBar", c); // classifier is bar
        test = test.add("barBaz", c); // classifier is bar
        
        Object[] fooStuff = test.getSuperSetWithClassifier("foo");
        Arrays.sort(fooStuff);
        assertEquals(2, fooStuff.length);
        assertEquals("fooBar", fooStuff[0]);
        assertEquals("fooBaz", fooStuff[1]);

        Object[] barStuff = test.getSuperSetWithClassifier("bar");
        Arrays.sort(barStuff);
        assertEquals(2, barStuff.length);
        assertEquals("barBar", barStuff[0]);
        assertEquals("barBaz", barStuff[1]);
        
        test = test.remove("fooBar", c);
        test = test.remove("fooBaz", c);
        test = test.remove("barBaz", c);
        
        fooStuff = test.getSuperSetWithClassifier("foo");
        assertEquals(0, fooStuff.length);

        barStuff = test.getSuperSetWithClassifier("bar");
        assertEquals(1, barStuff.length);
        assertEquals("barBar", barStuff[0]);
        
        test = test.remove("barBar", c);

        for (Object elem : test.elements) {
            assertNull(elem);
        }
        
        assertEquals(0, test.getOccupiedCells());
    }
    
    public void testResize() {
        CompactMultiSetMax<Integer, Integer> test;
        
        // Repeat the test 100 times
        final int numEntries = 10000;
        for (int numRun=0; numRun < 10; numRun++) {
            for (int factor : new int[] {10, 100, 1000}) {
                final int factorFinal = factor;
                ClassifierGetter<Integer, Integer> c = new ClassifierGetter<Integer, Integer>() {
                    @Override
                    public Integer getClassifier(Integer obj) {
                        return obj % (numEntries / factorFinal);
                    }
                };

                test = new CompactMultiSetMax<Integer, Integer>(4, false);

                for (int i=0; i < numEntries; i++) {
                    test = test.add(i, c);
                    
                    assertTrue(test.getOccupiedCells() > 0);
                }
                
                for (int key=0; key < numEntries / factor; key++) {
                    Object[] matches = test.getSuperSetWithClassifier(key);
        
                    int count = 0;
                    for (Object match : matches) {
                        if (((Integer)match) % (numEntries / factor) == key) {
                            count++;
                        }
                    }
                    
                    assertEquals(factor, count);
                }
                
                for (int i=0; i < numEntries/2; i++) {
                    test = test.remove(i, c);
                    
                    // Also remove some random things that can't be there
                    if (i % 13 == 0) {
                        //test = test.remove(-i, c);
                    }
                }
                
                for (int key=0; key < numEntries / factor; key++) {
                    Object[] matches = test.getSuperSetWithClassifier(key);
        
                    int count = 0;
                    for (Object match : matches) {
                        if (((Integer)match) % (numEntries / factor) == key) {
                            count++;
                        }
                    }
                    
                    assertEquals(factor / 2, count);
                }
                
                for (int i=numEntries/2; i < numEntries; i++) {
                    test = test.remove(i, c);
                }

                assertEquals(0, test.getOccupiedCells());

                for (Object elem : test.elements) {
                    assertNull("For factor " + factor + " and numEntries " + numEntries + " with elements " +  Arrays.asList(CompactSet.getElements(elem)) + " as " + elem, elem);
                }

                for (int key=0; key < numEntries; key++) {
                    Object[] matches = test.getSuperSetWithClassifier(key);
                    
                    assertEquals(0, matches.length);
                }
            }
        }
    }
}
