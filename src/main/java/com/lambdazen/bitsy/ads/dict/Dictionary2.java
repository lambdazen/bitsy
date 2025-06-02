package com.lambdazen.bitsy.ads.dict;

public class Dictionary2 extends PrimitiveDictionary implements Dictionary {
    public static final int CAPACITY = 2;

    String key0;
    Object value0;

    String key1;
    Object value1;

    // Expand constructor
    public Dictionary2(Dictionary1 base, String key, Object value) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        // Last key
        this.key1 = key;
        this.value1 = value;
    }

    // Contract constructor
    public Dictionary2(Dictionary3 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;
    }

    // Copy constructor
    public Dictionary2(Dictionary2 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;
    }

    // FromMap constructor
    public Dictionary2(String[] keys, Object[] values) {
        this.key0 = lookupKey(keys, 0);
        this.value0 = lookupValue(values, 0);

        this.key1 = lookupKey(keys, 1);
        this.value1 = lookupValue(values, 1);
    }

    @Override
    String[] keys() {
        return new String[] {key0, key1};
    }

    @Override
    Object[] values() {
        return new Object[] {value0, value1};
    }

    @Override
    void write(int index, String key, Object value) {
        switch (index) {
            case 0:
                key0 = key;
                value0 = value;
                break;

            case 1:
                key1 = key;
                value1 = value;
                break;

            default:
                throw new IllegalArgumentException("Invalid index " + index);
        }
    }

    @Override
    Dictionary expand(String key, Object value) {
        return new Dictionary3(this, key, value);
    }

    @Override
    int contractThreshold() {
        return Dictionary1.CAPACITY;
    }

    @Override
    Dictionary contract() {
        return new Dictionary1(this);
    }

    @Override
    public Dictionary copyOf() {
        return new Dictionary2(this);
    }
}
