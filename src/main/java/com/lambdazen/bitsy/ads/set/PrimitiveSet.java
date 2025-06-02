package com.lambdazen.bitsy.ads.set;

import java.util.Arrays;

public abstract class PrimitiveSet<T> implements Set<T> {
    public PrimitiveSet() {
        // Nothing to do
    }

    abstract Object[] elements();

    abstract void write(int index, T elem);

    abstract Set<T> expand(T elem);

    abstract int contractThreshold();

    abstract Object contract();

    public int size() {
        Object[] elems = elements();

        return size(elems);
    }

    private int size(Object[] elems) {
        int i;
        for (i = 0; i < elems.length; i++) {
            if (elems[i] == null) {
                break;
            }
        }

        return i;
    }

    public Object[] getElements() {
        Object[] elems = elements();

        int i;
        for (i = 0; i < elems.length; i++) {
            if (elems[i] == null) {
                break;
            }
        }

        return Arrays.copyOf(elems, i);
    }

    public Set<T> addElement(T elem) {
        Object[] elems = elements();
        int size = elems.length;

        boolean duplicate = false;
        int i;
        for (i = 0; i < size; i++) {
            T curElem = (T) elems[i];

            if (curElem == null) {
                // End of keys
                break;
            } else if (elems[i].equals(elem)) {
                duplicate = true;
            }
        }

        if (duplicate) {
            // Stick with this
            return this;
        } else {
            if (i == size) {
                // Reached end, need to move up
                return expand(elem);
            } else {
                // Not yet at the end
                write(i, elem);

                return this;
            }
        }
    }

    @Override
    public Object removeElement(T elem) {
        Object[] elems = elements();
        int size = elems.length;

        int overwritePos = -1;
        int i;
        for (i = 0; i < size; i++) {
            Object curElem = elems[i];

            if (curElem == null) {
                // End of keys
                break;
            } else if (curElem.equals(elem)) {
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
                write(overwritePos, (T) elems[lastIdx]);
            }
            write(lastIdx, null);

            if (lastIdx <= contractThreshold()) {
                // The new size is at or below the contract threshold
                return contract();
            } else {
                return this;
            }
        }
    }
}
