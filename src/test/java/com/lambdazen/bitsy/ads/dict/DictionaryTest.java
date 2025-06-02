package com.lambdazen.bitsy.ads.dict;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import junit.framework.TestCase;

public class DictionaryTest extends TestCase {
    private Random rand = new Random();

    public void testBasicExpandContract() {
        Dictionary dict = new Dictionary1("foo", "bar");

        assertEquals(1, dict.getPropertyKeys().length);
        assertEquals("foo", dict.getPropertyKeys()[0]);
        assertEquals("bar", dict.getProperty("foo"));

        dict = dict.setProperty("foo", "baz");

        assertEquals(Dictionary1.class, dict.getClass());
        assertEquals(1, dict.getPropertyKeys().length);
        assertEquals("foo", dict.getPropertyKeys()[0]);
        assertEquals("baz", dict.getProperty("foo"));

        // Move up
        dict = dict.setProperty("foo1", "bar1");

        assertEquals(Dictionary2.class, dict.getClass());
        assertEquals(2, dict.getPropertyKeys().length);
        assertEquals("baz", dict.getProperty("foo"));
        assertEquals("bar1", dict.getProperty("foo1"));

        // Move down
        dict = dict.removeProperty("foo");
        assertEquals(Dictionary1.class, dict.getClass());
        assertEquals(1, dict.getPropertyKeys().length);
        assertEquals("bar1", dict.getProperty("foo1"));

        // Move up
        dict = dict.setProperty("foo2", "bar2");

        assertEquals(Dictionary2.class, dict.getClass());
        assertEquals(2, dict.getPropertyKeys().length);
        assertEquals("bar1", dict.getProperty("foo1"));
        assertEquals("bar2", dict.getProperty("foo2"));

        // Overwrite foo2
        dict = dict.setProperty("foo2", "baz2");

        assertEquals(Dictionary2.class, dict.getClass());
        assertEquals(2, dict.getPropertyKeys().length);
        assertEquals("bar1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));

        // Move up to 3
        dict = dict.setProperty("foo3", "bar3");

        assertEquals(Dictionary3.class, dict.getClass());
        assertEquals(3, dict.getPropertyKeys().length);
        assertEquals("bar1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar3", dict.getProperty("foo3"));

        // Overwrite foo1
        dict = dict.setProperty("foo1", "baz1");

        assertEquals(Dictionary3.class, dict.getClass());
        assertEquals(3, dict.getPropertyKeys().length);
        assertEquals("baz1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar3", dict.getProperty("foo3"));

        // Move up to 4
        dict = dict.setProperty("foo4", "bar4");

        assertEquals(Dictionary4.class, dict.getClass());
        assertEquals(4, dict.getPropertyKeys().length);
        assertEquals("baz1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar3", dict.getProperty("foo3"));
        assertEquals("bar4", dict.getProperty("foo4"));

        // Move up to 5
        dict = dict.setProperty("foo5", "bar5");

        assertEquals(Dictionary6.class, dict.getClass());
        assertEquals(5, dict.getPropertyKeys().length);
        assertEquals("baz1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar3", dict.getProperty("foo3"));
        assertEquals("bar4", dict.getProperty("foo4"));
        assertEquals("bar5", dict.getProperty("foo5"));

        // Move up to 6
        dict = dict.setProperty("foo6", "bar6");

        assertEquals(Dictionary6.class, dict.getClass());
        assertEquals(6, dict.getPropertyKeys().length);
        assertEquals("baz1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar3", dict.getProperty("foo3"));
        assertEquals("bar4", dict.getProperty("foo4"));
        assertEquals("bar5", dict.getProperty("foo5"));
        assertEquals("bar6", dict.getProperty("foo6"));

        // Move down to 5
        dict = dict.removeProperty("invalidKey"); // invalid key
        dict = dict.removeProperty("foo3");

        assertEquals(Dictionary6.class, dict.getClass());
        assertEquals(5, dict.getPropertyKeys().length);
        assertEquals("baz1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar4", dict.getProperty("foo4"));
        assertEquals("bar5", dict.getProperty("foo5"));
        assertEquals("bar6", dict.getProperty("foo6"));

        // Move down to 4
        dict = dict.removeProperty("invalidKey"); // invalid key
        dict = dict.removeProperty("foo6");

        assertEquals(Dictionary4.class, dict.getClass());
        assertEquals(4, dict.getPropertyKeys().length);
        assertEquals("baz1", dict.getProperty("foo1"));
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar4", dict.getProperty("foo4"));
        assertEquals("bar5", dict.getProperty("foo5"));

        // Move down to 3
        dict = dict.removeProperty("invalidKey"); // invalid key
        dict = dict.removeProperty("foo1");

        assertEquals(Dictionary3.class, dict.getClass());
        assertEquals(3, dict.getPropertyKeys().length);
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar4", dict.getProperty("foo4"));
        assertEquals("bar5", dict.getProperty("foo5"));

        // Move down to 2
        dict = dict.removeProperty("invalidKey"); // invalid key
        dict = dict.removeProperty("foo4");

        assertEquals(Dictionary2.class, dict.getClass());
        assertEquals(2, dict.getPropertyKeys().length);
        assertEquals("baz2", dict.getProperty("foo2"));
        assertEquals("bar5", dict.getProperty("foo5"));

        // Move down to 1
        dict = dict.removeProperty("invalidKey"); // invalid key
        dict = dict.removeProperty("foo5");

        assertEquals(Dictionary1.class, dict.getClass());
        assertEquals(1, dict.getPropertyKeys().length);
        assertEquals("baz2", dict.getProperty("foo2"));

        // Move down to 0
        dict = dict.removeProperty("invalidKey"); // invalid key
        dict = dict.removeProperty("foo2");

        assertNull(dict);
    }

    public void testRandomExpandContract() {
        randomTestIter(5000, 171);
        randomTestIter(100000, 17);
    }

    public void randomTestIter(int numIters, int numKeys) {
        Map<String, Object> reference = new HashMap<String, Object>();

        Dictionary dict = null;

        for (int i = 0; i < numIters; i++) {
            String key = "key " + (i % numKeys);
            Object value = getRandomObject();

            if (rand.nextBoolean() || rand.nextBoolean()) {
                // Prob 0.75 to set
                if (dict == null) {
                    dict = new Dictionary1(key, value);
                } else {
                    dict = dict.setProperty(key, value);
                }

                reference.put(key, value);
            } else {
                // Prob 0.25 to remove
                if (dict != null) {
                    dict = dict.removeProperty(key);
                }

                reference.remove(key);
            }

            if ((rand.nextInt() % 2 == 0) && dict != null) {
                dict = dict.copyOf();
            }

            compareAgainstMap(dict, reference);
        }
    }

    public void compareAgainstMap(Dictionary dict, Map<String, Object> reference) {
        String[] keys = (dict == null) ? new String[0] : dict.getPropertyKeys();

        assertEquals(reference.size(), keys.length);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(reference.get(keys[i]), dict.getProperty(keys[i]));
        }

        if (rand.nextInt() % 2 == 0) {
            dict = DictionaryFactory.fromMap(reference);
        }
    }

    public Object getRandomObject() {
        long time = System.currentTimeMillis();

        if (rand.nextBoolean()) {
            if (rand.nextBoolean()) {
                return time;
            } else {
                return "" + time;
            }
        } else {
            if (rand.nextBoolean()) {
                return new StringBuilder("" + time);
            } else {
                return null;
            }
        }
    }
}
