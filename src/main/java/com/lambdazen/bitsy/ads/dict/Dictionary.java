package com.lambdazen.bitsy.ads.dict;

import com.lambdazen.bitsy.store.IStringCanonicalizer;

/**
 * This is an re-organizing (not immutable) map from String to Object. The set
 * and remove methods return a reference to a new map with the value.
 */
public interface Dictionary {
    public int size();

    public Object getProperty(String key);

    public String[] getPropertyKeys();

    public Dictionary setProperty(String key, Object value);

    public Dictionary removeProperty(String key);

    public Dictionary copyOf();

    //	public TreeMap<String, Object> toMap();

    public void canonicalizeKeys(IStringCanonicalizer canonicalizer);
}
