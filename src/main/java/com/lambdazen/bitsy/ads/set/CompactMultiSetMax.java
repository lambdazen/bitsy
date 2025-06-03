package com.lambdazen.bitsy.ads.set;

import java.util.ArrayList;
import java.util.List;

/**
 * The compact multi-set takes an element and a classifier on that element. It
 * supports a get that takes the classifier and returns the matches. This class
 * is NOT thread-safe, but can support multiple readers as long as there is only
 * one writer.
 *
 * This class is used for two purposes. The first is to store adjacency lists by
 * label. The classifier picks up the label from the Edge. The second purpose is
 * to provide SetMax with a thread-safe HashSet implementation.
 */
public class CompactMultiSetMax<C, T> {
    public static final int MIN_TO_RESIZE = 8;

    int occupied = 0;
    boolean safe;
    Object[] elements;

    public CompactMultiSetMax(int initSize, boolean safe) {
        this.elements = new Object[initSize];
        this.occupied = 0;
        this.safe = safe;
    }

    public int getOccupiedCells() {
        return this.occupied;
    }

    public CompactMultiSetMax<C, T> add(T obj, ClassifierGetter<C, T> c) {
        Object classifier = c.getClassifier(obj);

        addElementNoRehash(classifier.hashCode(), obj);

        int len = elements.length;
        if (occupied >= len - (len / 4)) { // 0.75 load factor
            return rehash(len * 2, c);
        } else {
            return this;
        }
    }

    protected void addElementNoRehash(int hashCode, T obj) {
        int index = (hashCode & 0x7FFFFFFF) % elements.length;

        if (elements[index] == null) {
            occupied++;
        }

        if (safe) {
            elements[index] = CompactSet.<T>addSafe(elements[index], obj);
        } else {
            elements[index] = CompactSet.<T>add(elements[index], obj);
        }
    }

    private CompactMultiSetMax<C, T> rehash(int newLength, ClassifierGetter<C, T> c) {
        CompactMultiSetMax<C, T> ans = new CompactMultiSetMax<C, T>(newLength, safe); // use the same safe boolean

        for (Object elem : elements) {
            for (Object item : CompactSet.getElements(elem)) {
                Object classifier = c.getClassifier((T) item);
                ans.addElementNoRehash(classifier.hashCode(), (T) item);
            }
        }

        return ans;
    }

    public CompactMultiSetMax<C, T> remove(T obj, ClassifierGetter<C, T> c) {
        Object classifier = c.getClassifier(obj);

        removeElementNoHash(classifier.hashCode(), obj);

        int len = elements.length;
        if ((occupied > MIN_TO_RESIZE) && (occupied < len / 2)) {
            return rehash(len / 2, c);
        } else {
            return this;
        }
    }

    protected void removeElementNoHash(int hashCode, T obj) {
        int index = (hashCode & 0x7FFFFFFF) % elements.length;

        Object oldVal = elements[index];
        if (oldVal == null) {
            return;
        } else {
            Object newVal = CompactSet.<T>remove(oldVal, obj);
            elements[index] = newVal;
            if (newVal == null) {
                occupied--;
            }
        }
    }

    /*
     * Returns a CompactSet-compatible object with the given classifier. Note that
     * extra elements could be returned -- hence "super set". It is the
     * responsibility of the caller to weed these out
     */
    public Object[] getSuperSetWithClassifier(C key) {
        if (key == null) {
            return getAllElements();
        } else {
            int index = (key.hashCode() & 0x7FFFFFFF) % elements.length;

            return CompactSet.getElements(elements[index]);
        }
    }

    public Object[] getAllElements() {
        List<Object> ans = new ArrayList<Object>();

        for (int i = 0; i < elements.length; i++) {
            Object elem = elements[i];

            for (Object item : CompactSet.getElements(elem)) {
                if (item != null) {
                    // This check is needed because item could be null when
                    // dealing with lock-free reads that occur during a write.
                    // The system will retry the read based on the sequence
                    // number -- the important thing is to
                    // not throw an exception
                    ans.add(item);
                }
            }
        }

        return ans.toArray();
    }

    // This method goes over the elements to see if this compact set can be fit inside a Set24
    public boolean sizeBiggerThan24() {
        int currentSize = 0;
        for (int i = 0; i < elements.length; i++) {
            Object elem = elements[i];
            if (elem == null) {
                // Empty cell
                continue;
            } else if (elem instanceof ArraySet) {
                // Don't reorg till the ArraySet reduces to Set24
                return true;
            } else if (elem instanceof SetMax) {
                // Don't reorg till the SetMax reduces to Set24
                return true;
            } else {
                currentSize += CompactSet.size(elem);
            }

            if (currentSize > 24) {
                return true;
            }
        }

        return false;
    }
}
