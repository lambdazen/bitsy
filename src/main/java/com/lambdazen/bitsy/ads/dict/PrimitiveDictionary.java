package com.lambdazen.bitsy.ads.dict;

import java.util.Arrays;

import com.lambdazen.bitsy.store.IStringCanonicalizer;

public abstract class PrimitiveDictionary implements Dictionary {
	public PrimitiveDictionary() {
		// Nothing to do
	}

	abstract String[] keys();
	abstract Object[] values();
	abstract void write(int index, String key, Object value);
	abstract Dictionary expand(String key, Object value);
	abstract int contractThreshold();
	abstract Dictionary contract();

	public abstract Dictionary copyOf();

	public int size() {
		String[] keys = keys();
		
		int i;
		for (i=0; i < keys.length; i++) {
			if (keys[i] == null) {
				break;
			}
		}
		
		return i;
	}
	
	public String[] getPropertyKeys() {
		String[] keys = keys();
		
		int i;
		for (i=0; i < keys.length; i++) {
			if (keys[i] == null) {
				break;
			}
		}
		
		return Arrays.copyOf(keys, i);
	}

	@Override
	public Object getProperty(String key) {
		String[] keys = keys();
		Object[] values = values();
		
		for (int i=0; i < keys.length; i++) {
			String curKey = keys[i];
			
			if (curKey == null) {
				// End of keys
				return null;
			} else if (curKey.equals(key)) {
				return values[i];
			}
		}
		
		return null;
	}
	
	@Override
	public Dictionary setProperty(String key, Object value) {
		String[] keys = keys();
		Object[] values = values();
		
		boolean overwroteValue = false;
		int i;
		for (i=0; i < keys.length; i++) {
			String curKey = keys[i];
			
			if (curKey == null) {
				// End of keys
				break;
			} else if (keys[i].equals(key)) {
				values[i] = value;
				write(i, keys[i], value);
				overwroteValue = true;
			}
		}
		
		if (overwroteValue) {
			// Stick with this
			return this;
		} else {
			if (i == keys.length) {
				// Reached end, need to move up
				return expand(key, value);
			} else {
				// Not yet at the end
				write(i, key, value);
				
				return this;
			}
		}
	}
	
	@Override
	public void canonicalizeKeys(IStringCanonicalizer canonicalizer) {
	    String[] keys = keys();
	    Object[] values = null;
	    
	    int i = 0;
        for (i=0; i < keys.length; i++) {
            String origKey = keys[i];
            String newKey = canonicalizer.canonicalize(origKey);

	        // Avoid step if already canonical
	        if (newKey != origKey) {
	            if (values == null) {
	                // Don't generate values unless required
	                values = values();
	            }

	            write(i, newKey, values[i]);
	        }
	        
	        i++;
	    }
	}
	
	@Override
	public Dictionary removeProperty(String key) {
		String[] keys = keys();
		Object[] values = values();
		
		int overwritePos = -1;
		int i;
		for (i=0; i < keys.length; i++) {
			String curKey = keys[i];
			
			if (curKey == null) {
				// End of keys
				break;
			} else if (keys[i].equals(key)) {
				overwritePos = i;
			}
		}
		
		if (overwritePos == -1) {
			// Couldn't find key
			return this;
		} else {
			// Overwrite from end to here
			int lastIdx = i - 1;
			if (overwritePos != lastIdx) {
				write(overwritePos, keys[lastIdx], values[lastIdx]);
			}
			write(lastIdx, null, null);

			if (lastIdx <= contractThreshold()) {
				// The new size is at or below the contract threshold
				return contract();
			} else {
				return this;
			}
		}
	}
	
	protected String lookupKey(String[] keys, int i) {
	    return (i < keys.length) ? keys[i] : null;
	}

	protected Object lookupValue(Object[] values, int i) {
	    return (i < values.length) ? values[i] : null;
	}

	public String toString() {
	    StringBuffer ans = new StringBuffer("PrimitiveDictionary(size = " + size());
	    for (String key : getPropertyKeys()) {
	        ans.append(", " + key + ": " + getProperty(key));
	    }
	    ans.append(")");
	    return ans.toString();
	}
}
