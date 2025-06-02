package com.lambdazen.bitsy.ads.dict;

public class Dictionary11 extends PrimitiveDictionary implements Dictionary {
    public static final int CAPACITY = 11;

    String key0;
    Object value0;

    String key1;
    Object value1;

    String key2;
    Object value2;

    String key3;
    Object value3;

    String key4;
    Object value4;

    String key5;
    Object value5;

    String key6;
    Object value6;

    String key7;
    Object value7;

    String key8;
    Object value8;

    String key9;
    Object value9;

    String key10;
    Object value10;

    // Expand constructor
    public Dictionary11(Dictionary8 base, String key, Object value) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;

        this.key2 = base.key2;
        this.value2 = base.value2;

        this.key3 = base.key3;
        this.value3 = base.value3;

        this.key4 = base.key4;
        this.value4 = base.value4;

        this.key5 = base.key5;
        this.value5 = base.value5;

        this.key6 = base.key6;
        this.value6 = base.value6;

        this.key7 = base.key7;
        this.value7 = base.value7;

        // Last key
        this.key8 = key;
        this.value8 = value;
    }

    // Contract constructor
    public Dictionary11(Dictionary16 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;

        this.key2 = base.key2;
        this.value2 = base.value2;

        this.key3 = base.key3;
        this.value3 = base.value3;

        this.key4 = base.key4;
        this.value4 = base.value4;

        this.key5 = base.key5;
        this.value5 = base.value5;

        this.key6 = base.key6;
        this.value6 = base.value6;

        this.key7 = base.key7;
        this.value7 = base.value7;

        this.key8 = base.key8;
        this.value8 = base.value8;

        this.key9 = base.key9;
        this.value9 = base.value9;

        this.key10 = base.key10;
        this.value10 = base.value10;
    }

    // Copy constructor
    public Dictionary11(Dictionary11 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;

        this.key1 = base.key1;
        this.value1 = base.value1;

        this.key2 = base.key2;
        this.value2 = base.value2;

        this.key3 = base.key3;
        this.value3 = base.value3;

        this.key4 = base.key4;
        this.value4 = base.value4;

        this.key5 = base.key5;
        this.value5 = base.value5;

        this.key6 = base.key6;
        this.value6 = base.value6;

        this.key7 = base.key7;
        this.value7 = base.value7;

        this.key8 = base.key8;
        this.value8 = base.value8;

        this.key9 = base.key9;
        this.value9 = base.value9;

        this.key10 = base.key10;
        this.value10 = base.value10;
    }

    // FromMap constructor
    public Dictionary11(String[] keys, Object[] values) {
        this.key0 = lookupKey(keys, 0);
        this.value0 = lookupValue(values, 0);

        this.key1 = lookupKey(keys, 1);
        this.value1 = lookupValue(values, 1);

        this.key2 = lookupKey(keys, 2);
        this.value2 = lookupValue(values, 2);

        this.key3 = lookupKey(keys, 3);
        this.value3 = lookupValue(values, 3);

        this.key4 = lookupKey(keys, 4);
        this.value4 = lookupValue(values, 4);

        this.key5 = lookupKey(keys, 5);
        this.value5 = lookupValue(values, 5);

        this.key6 = lookupKey(keys, 6);
        this.value6 = lookupValue(values, 6);

        this.key7 = lookupKey(keys, 7);
        this.value7 = lookupValue(values, 7);

        this.key8 = lookupKey(keys, 8);
        this.value8 = lookupValue(values, 8);

        this.key9 = lookupKey(keys, 9);
        this.value9 = lookupValue(values, 9);

        this.key10 = lookupKey(keys, 10);
        this.value10 = lookupValue(values, 10);
    }

    @Override
    String[] keys() {
        return new String[] {key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10};
    }

    @Override
    Object[] values() {
        return new Object[] {value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10};
    }

    @Override
    void write(int index, String key, Object value) {
        if (index < 6) {
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

                case 4:
                    key4 = key;
                    value4 = value;
                    break;

                case 5:
                    key5 = key;
                    value5 = value;
                    break;

                default:
                    throw new RuntimeException("Bug in code");
            }
        } else {
            switch (index) {
                case 6:
                    key6 = key;
                    value6 = value;
                    break;

                case 7:
                    key7 = key;
                    value7 = value;
                    break;

                case 8:
                    key8 = key;
                    value8 = value;
                    break;

                case 9:
                    key9 = key;
                    value9 = value;
                    break;

                case 10:
                    key10 = key;
                    value10 = value;
                    break;

                default:
                    throw new IllegalArgumentException("Invalid index " + index);
            }
        }
    }

    @Override
    Dictionary expand(String key, Object value) {
        return new Dictionary16(this, key, value);
    }

    @Override
    int contractThreshold() {
        return Dictionary8.CAPACITY;
    }

    @Override
    Dictionary contract() {
        return new Dictionary8(this);
    }

    @Override
    public Dictionary copyOf() {
        return new Dictionary11(this);
    }
}
