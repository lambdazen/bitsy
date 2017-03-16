package com.lambdazen.bitsy.ads.dict;

import java.util.Map;

public class DictionaryFactory {
    public static Dictionary fromMap(Map<String, Object> properties) {
        if (properties == null) {
            return null;
        }

        int size = properties.size();
        String[] keys = new String[size];
        Object[] values = new Object[size];

        int counter = 0;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            keys[counter] = entry.getKey();
            values[counter] = entry.getValue();
            counter++;
        }
        
        //assert counter == size;
        
        if (size == 0) {
            return null;
        } else if (size <= 1) {
            return new Dictionary1(keys[0], values[0]);
        } else if (size <= 2) {
            return new Dictionary2(keys, values);
        } else if (size <= 3) {
            return new Dictionary3(keys, values);
        } else if (size <= 4) {
            return new Dictionary4(keys, values);
        } else if (size <= 6) {
            return new Dictionary6(keys, values);
        } else if (size <= 8) {
            return new Dictionary8(keys, values);
        } else if (size <= 11) {
            return new Dictionary11(keys, values);
        } else if (size <= 16) {
            return new Dictionary16(keys, values);
        } else {
            return new DictionaryMax(keys, values);
        }
    }
}
