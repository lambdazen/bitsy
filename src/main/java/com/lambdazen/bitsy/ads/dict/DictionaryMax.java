package com.lambdazen.bitsy.ads.dict;

import java.util.Arrays;

public class DictionaryMax extends PrimitiveDictionary implements Dictionary {
	int capacity;
	String[] keys;
	Object[] values;

	// Expand constructor
	public DictionaryMax(Dictionary16 base, String key, Object value) {
		this.capacity = 24;
		keys = Arrays.copyOf(base.keys(), capacity);
		values = Arrays.copyOf(base.values(), capacity);
		
		keys[16] = key;
		values[16] = value;
	}

	// Copy constructor
	public DictionaryMax(DictionaryMax base) {
	    this.capacity = base.capacity;
	    keys = Arrays.copyOf(base.keys(), capacity);
	    values = Arrays.copyOf(base.values(), capacity);
	}

    // FromMap constructor
    public DictionaryMax(String[] keys, Object[] values) {
        this.capacity = Math.max(24, keys.length + keys.length / 2);

        this.keys = Arrays.copyOf(keys, capacity);
        this.values = Arrays.copyOf(values, capacity);
    }
    
	@Override
	String[] keys() {
		return keys;
	}

	@Override
	Object[] values() {
		return values;
	}

	@Override
	void write(int index, String key, Object value) {
		keys[index] = key;
		values[index] = value;
	}

	@Override
	Dictionary expand(String key, Object value) {
		int newCapacity = capacity + (capacity / 2);
		keys = Arrays.copyOf(keys, newCapacity);
		values = Arrays.copyOf(values, newCapacity);
		
		keys[capacity] = key;
		values[capacity] = value;
		
		this.capacity = newCapacity;
		
		return this;
	}

	@Override
	int contractThreshold() {
		return capacity / 2;
	}

	@Override
	Dictionary contract() {
		if (capacity < 14) {
			// Move to Dictionary16
			return new Dictionary16(this);
		} else {
			int newCapacity = capacity * 3 / 4;
			keys = Arrays.copyOf(keys, newCapacity);
			values = Arrays.copyOf(values, newCapacity);			
			this.capacity = newCapacity;
			
			return this;
		}
	}

    @Override
    public Dictionary copyOf() {
        return new DictionaryMax(this);
    }
}
