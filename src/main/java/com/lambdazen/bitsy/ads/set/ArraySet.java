package com.lambdazen.bitsy.ads.set;

import java.util.Arrays;

/**
 * This class uses an array-based set implementation rather than SetMax and
 * CompactMultiSetMax classes that implement a hash-based set. Neither
 * implementation throws ConcurrentModificationException on reads, but expect
 * writes to be serialized.
 */
public class ArraySet<T> implements Set<T> {
    int size;
    Object[] elements; 

    public ArraySet(Object[] elements) {
        this(elements, elements.length);
    }

    protected ArraySet(Object[] elements, int size) {
        this.size = size;
        this.elements = new Object[size + size / 2];

        for (int i=0; i < size; i++) {
            this.elements[i] = (T)elements[i];
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Object[] getElements() {
        return Arrays.copyOf(elements, size);
    }

    @Override
    public Object removeElement(T elem) {
        // Go over elements and remove the one
        for (int i=0; i < size; i++) {
            if (elem.equals(elements[i])) {
                if (i < size - 1) {
                    elements[i] = elements[size - 1];
                }

                this.size--;
                elements[size] = null;
                
                break;
            }
        }
        
        if (size < 16) {
            return new Set24<T>(getElements());
        } else if (size < elements.length / 2) {
            // Using the constructor that cuts the size -- to avoid two array creations
            return new ArraySet<T>(elements, size);
        } else {
            // Use the same object 
            return this;
        }
    }

    @Override
    public Set<T> addElement(T elem) {
        for (int i=0; i < size; i++) {
            if (elem.equals(elements[i])) {
                // Nothing to do
                return this;
            }
        }
        
        if (size < elements.length) {
            elements[size] = elem;
            this.size++;
            return this;
        } else {
            Set<T> ans = new ArraySet<T>(elements);
            ans.addElement(elem);
            return ans;
        }
    }
}
