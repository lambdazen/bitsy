package com.lambdazen.bitsy.ads.dict;

import java.util.Arrays;

public class Dictionary16 extends PrimitiveDictionary implements Dictionary {
	public static final int CAPACITY = 16;
	
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
	
	String key11;
	Object value11;
	
	String key12;
	Object value12;
	
	String key13;
	Object value13;
	
	String key14;
	Object value14;

	String key15;
	Object value15;

	// Expand constructor
	public Dictionary16(Dictionary11 base, String key, Object value) {
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
		
		// Last key
		this.key11 = key;
		this.value11 = value;
	}

	// Contract constructor
	public Dictionary16(DictionaryMax base) {
		String[] keys = Arrays.copyOf(base.keys(), 16);
		Object[] values = Arrays.copyOf(base.values(), 16);
		
		this.key0 = keys[0];
		this.value0 = values[0];

		this.key1 = keys[1];
		this.value1 = values[1];
		
		this.key2 = keys[2];
		this.value2 = values[2];

		this.key3 = keys[3];
		this.value3 = values[3];
		
		this.key4 = keys[4];
		this.value4 = values[4];

		this.key5 = keys[5];
		this.value5 = values[5];
		
		this.key6 = keys[6];
		this.value6 = values[6];

		this.key7 = keys[7];
		this.value7 = values[7];
		
		this.key8 = keys[8];
		this.value8 = values[8];

		this.key9 = keys[9];
		this.value9 = values[9];
		
		this.key10 = keys[10];
		this.value10 = values[10];

		this.key11 = keys[11];
		this.value11 = values[11];
		
		this.key12 = keys[12];
		this.value12 = values[12];

		this.key13 = keys[13];
		this.value13 = values[13];
		
		this.key14 = keys[14];
		this.value14 = values[14];

		this.key15 = keys[15];
		this.value15 = values[15];
	}
	
	// Copy constructor
	public Dictionary16(Dictionary16 base) {
        String[] keys = Arrays.copyOf(base.keys(), 16);
        Object[] values = Arrays.copyOf(base.values(), 16);
        
        this.key0 = keys[0];
        this.value0 = values[0];

        this.key1 = keys[1];
        this.value1 = values[1];
        
        this.key2 = keys[2];
        this.value2 = values[2];

        this.key3 = keys[3];
        this.value3 = values[3];
        
        this.key4 = keys[4];
        this.value4 = values[4];

        this.key5 = keys[5];
        this.value5 = values[5];
        
        this.key6 = keys[6];
        this.value6 = values[6];

        this.key7 = keys[7];
        this.value7 = values[7];
        
        this.key8 = keys[8];
        this.value8 = values[8];

        this.key9 = keys[9];
        this.value9 = values[9];
        
        this.key10 = keys[10];
        this.value10 = values[10];

        this.key11 = keys[11];
        this.value11 = values[11];
        
        this.key12 = keys[12];
        this.value12 = values[12];

        this.key13 = keys[13];
        this.value13 = values[13];
        
        this.key14 = keys[14];
        this.value14 = values[14];

        this.key15 = keys[15];
        this.value15 = values[15];
    }


    // FromMap constructor
    public Dictionary16(String[] keys, Object[] values) {
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

        this.key11 = lookupKey(keys, 11);
        this.value11 = lookupValue(values, 11);

        this.key12 = lookupKey(keys, 12);
        this.value12 = lookupValue(values, 12);

        this.key13 = lookupKey(keys, 13);
        this.value13 = lookupValue(values, 13);

        this.key14 = lookupKey(keys, 14);
        this.value14 = lookupValue(values, 14);

        this.key15 = lookupKey(keys, 15);
        this.value15 = lookupValue(values, 15);
    }
    
	@Override
	String[] keys() {
		return new String[] {key0, key1, key2, key3, key4, key5, key6, key7, key8, key9, key10, key11, key12, key13, key14, key15};
	}

	@Override
	Object[] values() {
		return new Object[] {value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12, value13, value14, value15};
	}

	@Override
	void write(int index, String key, Object value) {
		if (index < 4) {
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
				throw new RuntimeException("Bug in code");
			}
		} else if (index < 8){
			switch (index) {

			case 4:
				key4 = key;
				value4 = value;
				break;

			case 5: 
				key5 = key;
				value5 = value;
				break;
				
			case 6: 
				key6 = key;
				value6 = value;
				break;

			case 7: 
				key7 = key;
				value7 = value;
				break;

			default:
				throw new RuntimeException("Bug in code");
			}
		} else if (index < 12 ) {
			switch (index) {

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

			case 11: 
				key11 = key;
				value11 = value;
				break;
				
			default: 
				throw new RuntimeException("Bug in code");
			}
		} else {
			switch (index) {

			case 12: 
				key12 = key;
				value12 = value;
				break;

			case 13: 
				key13 = key;
				value13 = value;
				break;

			case 14: 
				key14 = key;
				value14 = value;
				break;

			case 15: 
				key15 = key;
				value15 = value;
				break;

			default:
				throw new IllegalArgumentException("Invalid index " + index);
			}
		}
	}

	@Override
	Dictionary expand(String key, Object value) {
		return new DictionaryMax(this, key, value);
	}

	@Override
	int contractThreshold() {
		return Dictionary11.CAPACITY;
	}

	@Override
	Dictionary contract() {
		return new Dictionary11(this);
	}
    
    @Override
    public Dictionary copyOf() {
        return new Dictionary16(this);
    }
}
