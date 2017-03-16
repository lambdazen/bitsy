package com.lambdazen.bitsy.ads.dict;

public class Dictionary3 extends PrimitiveDictionary implements Dictionary {
	public static final int CAPACITY = 3;
	
	String key0;
	Object value0;
	
	String key1;
	Object value1;

	String key2;
	Object value2;
	
	// Expand constructor
	public Dictionary3(Dictionary2 base, String key, Object value) {
		this.key0 = base.key0;
		this.value0 = base.value0;
		
		this.key1 = base.key1;
		this.value1 = base.value1;
		
		// Last key
		this.key2 = key;
		this.value2 = value;
	}

	// Contract constructor
	public Dictionary3(Dictionary4 base) {
		this.key0 = base.key0;
		this.value0 = base.value0;
		
		this.key1 = base.key1;
		this.value1 = base.value1;
		
		this.key2 = base.key2;
		this.value2 = base.value2;
	}
	
	// Copy constructor
	public Dictionary3(Dictionary3 base) {
        this.key0 = base.key0;
        this.value0 = base.value0;
        
        this.key1 = base.key1;
        this.value1 = base.value1;
        
        this.key2 = base.key2;
        this.value2 = base.value2;
    }
	
    // FromMap constructor
    public Dictionary3(String[] keys, Object[] values) {
        this.key0 = lookupKey(keys, 0);
        this.value0 = lookupValue(values, 0);

        this.key1 = lookupKey(keys, 1);
        this.value1 = lookupValue(values, 1);

        this.key2 = lookupKey(keys, 2);
        this.value2 = lookupValue(values, 2);
    }

	@Override
	String[] keys() {
		return new String[] {key0, key1, key2};
	}

	@Override
	Object[] values() {
		return new Object[] {value0, value1, value2};
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
			
		default: 
			throw new IllegalArgumentException("Invalid index " + index);
		}
	}

	@Override
	Dictionary expand(String key, Object value) {
		return new Dictionary4(this, key, value);
	}

	@Override
	int contractThreshold() {
		return Dictionary2.CAPACITY;
	}

	@Override
	Dictionary contract() {
	    return new Dictionary2(this);
	}
	
	@Override
	public Dictionary copyOf() {
        return new Dictionary3(this);
    }
}
