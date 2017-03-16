package com.lambdazen.bitsy.ads.dict;

public class Dictionary6 extends PrimitiveDictionary implements Dictionary {
	public static final int CAPACITY = 6;
	
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
	
	// Expand constructor
	public Dictionary6(Dictionary4 base, String key, Object value) {
		this.key0 = base.key0;
		this.value0 = base.value0;
		
		this.key1 = base.key1;
		this.value1 = base.value1;
		
		this.key2 = base.key2;
		this.value2 = base.value2;

		this.key3 = base.key3;
		this.value3 = base.value3;

		// Last key
		this.key4 = key;
		this.value4 = value;
	}

    // Contract constructor
	public Dictionary6(Dictionary8 base) {
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
	}

	// Copy constructor
    public Dictionary6(Dictionary6 base) {
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
    }
    
    // FromMap constructor
    public Dictionary6(String[] keys, Object[] values) {
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
    }
    
    @Override
	String[] keys() {
		return new String[] {key0, key1, key2, key3, key4, key5};
	}

	@Override
	Object[] values() {
		return new Object[] {value0, value1, value2, value3, value4, value5};
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
			
		case 4:
			key4 = key;
			value4 = value;
			break;
			
		case 5: 
			key5 = key;
			value5 = value;
			break;
			
		default: 
			throw new IllegalArgumentException("Invalid index " + index);
		}
	}

	@Override
	Dictionary expand(String key, Object value) {
		return new Dictionary8(this, key, value);
	}

	@Override
	int contractThreshold() {
		return Dictionary4.CAPACITY;
	}

	@Override
	Dictionary contract() {
		return new Dictionary4(this);
	}

	@Override
	public Dictionary copyOf() {
        return new Dictionary6(this);
    }
}
