package com.lambdazen.bitsy.ads.dict;

public class Dictionary4 extends PrimitiveDictionary implements Dictionary {
    public static final int CAPACITY = 4;

    String key0;
    Object value0;

    String key1;
    Object value1;

    String key2;
    Object value2;

    String key3;
    Object value3;

    // Expand constructor
    public Dictionary4(Dictionary3 base, String key, Object value) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;

        this.key2 = base.key2;
        this.value2 = base.value2;

        // Last key
        this.key3 = key;
        this.value3 = value;
    }

    // Contract constructor
    public Dictionary4(Dictionary6 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;

        this.key2 = base.key2;
        this.value2 = base.value2;

        this.key3 = base.key3;
        this.value3 = base.value3;
    }

    // Copy constructor
    public Dictionary4(Dictionary4 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;

        this.key2 = base.key2;
        this.value2 = base.value2;

        this.key3 = base.key3;
        this.value3 = base.value3;
    }

    // FromMap constructor
    public Dictionary4(String[] keys, Object[] values) {
        this.key0 = lookupKey(keys, 0);
        this.value0 = lookupValue(values, 0);

        this.key1 = lookupKey(keys, 1);
        this.value1 = lookupValue(values, 1);

        this.key2 = lookupKey(keys, 2);
        this.value2 = lookupValue(values, 2);

        this.key3 = lookupKey(keys, 3);
        this.value3 = lookupValue(values, 3);
    }

    @Override
    String[] keys() {
        return new String[] {key0, key1, key2, key3};
    }

    @Override
    Object[] values() {
        return new Object[] {value0, value1, value2, value3};
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

            case 2:
                key2 = key;
                value2 = value;
                break;

            case 3:
                key3 = key;
                value3 = value;
                break;

            default:
                throw new IllegalArgumentException("Invalid index " + index);
        }
    }

    @Override
    Dictionary expand(String key, Object value) {
        return new Dictionary6(this, key, value);
    }

    @Override
    int contractThreshold() {
        return Dictionary3.CAPACITY;
    }

    @Override
    Dictionary contract() {
        return new Dictionary3(this);
    }

    @Override
    public Dictionary copyOf() {
        return new Dictionary4(this);
    }
}
