package com.lambdazen.bitsy.index;

import com.lambdazen.bitsy.ads.set.CompactSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class BitsyIndex<T> {
    Map<Object, Object> index;

    public BitsyIndex() {
        this.index = new ConcurrentHashMap<Object, Object>();
    }

    public abstract Object getValue(T bean);

    public abstract T copy(T bean);

    public void load(Iterator<T> initialContents) {
        while (initialContents.hasNext()) {
            T elem = initialContents.next();
            add(elem);
        }
    }

    public List<T> get(Object value) {
        Object idxValue = index.get(value);

        if (idxValue == null) {
            return Collections.emptyList();
        } else {
            Object[] objs = CompactSet.getElements(idxValue);
            List<T> ans = new ArrayList<T>(objs.length);
            int len = objs.length;
            for (int i = 0; i < len; i++) {
                // Always check for nulls on getElements() because reads don't acquire locks
                if (objs[i] != null) {
                    ans.add(copy((T) objs[i]));
                }
            }

            return ans;
        }
    }

    public void add(T bean) {
        Object value = getValue(bean);
        if (value == null) {
            // Nothing to do
            return;
        }

        // No need to synchronize, because there is a read-write lock
        Object origSet = index.get(value);
        Object newSet = CompactSet.<T>add(origSet, bean);

        if (origSet != newSet) {
            index.put(value, newSet);
        }
    }

    public void remove(T bean) {
        Object value = getValue(bean);
        if (value == null) {
            // Nothing to do
            return;
        }

        // No need to synchronize, because there is a read-write lock
        Object origSet = index.get(value);
        Object newSet = CompactSet.<T>remove(origSet, bean);

        if (origSet != newSet) {
            if (newSet == null) {
                index.remove(value);
            } else {
                index.put(value, newSet);
            }
        }
    }
}
