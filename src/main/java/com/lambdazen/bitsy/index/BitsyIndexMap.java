package com.lambdazen.bitsy.index;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;

public class BitsyIndexMap<BeanType, IndexType extends BitsyIndex<BeanType>> {
    // Read operations on IndexMap don't require locks. 
    Map<String, IndexType> indexMap;
    
    public BitsyIndexMap() {
        this.indexMap = new ConcurrentHashMap<String, IndexType>();
    }
    

    // LOCK-FREE methods: The member indexNames can not be used.
    /** This method returns a copy of the edges/vertices held for the given key and value */
    public Collection<BeanType> get(String key, Object value) {
        IndexType index = indexMap.get(key);
        
        if (index == null) {
            throw new BitsyException(BitsyErrorCodes.MISSING_INDEX, "An index on " + key + " must be created before querying vertices/edges by that key. Defined indexes: " + indexMap.keySet());
        } else {
            return index.get(value);
        }
    }
    
    // LOCKED methods
    public void add(BeanType bean) {
        for (IndexType index : indexMap.values()) {
            index.add(bean);
        }
    }
    
    public void remove(BeanType bean) {
        if (bean == null) {
            // Nothing to do
            return;
        }
        
        for (IndexType index : indexMap.values()) {
            index.remove(bean);
        }
    }

    protected void addKeyIndex(String key, IndexType index) {
        if (indexMap.containsKey(key)) {
            throw new BitsyException(BitsyErrorCodes.INDEX_ALREADY_EXISTS, "Index on vertex key '" + key + "'");
        }

        indexMap.put(key, index);
    }
    
    public void dropKeyIndex(String key) {
        indexMap.remove(key);
    }
    
    public Set<String> getIndexedKeys() {
        return new HashSet<String>(indexMap.keySet());
    }
}
