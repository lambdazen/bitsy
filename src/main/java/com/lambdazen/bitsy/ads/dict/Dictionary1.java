package com.lambdazen.bitsy.ads.dict;

/** This class implements a dictionary with one element */
public class Dictionary1 extends PrimitiveDictionary implements Dictionary {
    public static final int CAPACITY = 1;

    String key0;
    Object value0;

    // Expand constructor
    public Dictionary1(String key, Object value) {
        this.key0 = key;
        this.value0 = value;
    }

    // Contract constructor
    public Dictionary1(Dictionary2 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;
    }

    // Copy constructor
    public Dictionary1(Dictionary1 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;
    }

    @Override
    protected String[] keys() {
        return new String[] {key0};
    }

    @Override
    protected Object[] values() {
        return new Object[] {value0};
    }

    @Override
    public Dictionary copyOf() {
        return new Dictionary1(this);
    }

    protected int contractThreshold() {
        return 0;
    }

    protected Dictionary contract() {
        return null;
    }

    protected Dictionary expand(String key, Object value) {
        return new Dictionary2(this, key, value);
    }

    protected void write(int index, String key, Object value) {
        key0 = key;
        value0 = value;
    }
}
