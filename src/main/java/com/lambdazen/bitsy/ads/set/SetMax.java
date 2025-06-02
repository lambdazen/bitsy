package com.lambdazen.bitsy.ads.set;

public class SetMax<T> implements Set<T> {
    private static final long serialVersionUID = 129583038274146507L;

    private static final ClassifierGetter identityClassifer = new ClassifierGetter() {
        @Override
        public Object getClassifier(Object obj) {
            return obj;
        }
    };

    int size;
    CompactMultiSetMax<T, T> hashSet;

    public SetMax(Set24<T> oldSet, Object elem) {
        // When using a compact multi-set inside a set, it is better to use
        // safe-mode to avoid a cyclic dependency when all hash-codes are same
        hashSet = new CompactMultiSetMax<T, T>(32, true);

        Object[] elems = oldSet.getElements();
        for (int i = 0; i < elems.length; i++) {
            T curElem = (T) elems[i];
            hashSet.addElementNoRehash(curElem.hashCode(), curElem);
        }

        T curElem = (T) elem;
        hashSet.addElementNoRehash(curElem.hashCode(), curElem);

        this.size = 1 + elems.length;

        assert (this.size == 25);
    }

    @Override
    public int size() {
        // Expensive operation -- could be inaccurate for lock-free reads
        // Doesn't matter because the read will retry
        return getElements().length;
    }

    @Override
    public Object[] getElements() {
        return hashSet.getAllElements();
    }

    @Override
    public Set<T> removeElement(T elem) {
        if (hashSet.elements.length <= 32) {
            // Don't resize under 32 -- Better to move to Set24
            hashSet.removeElementNoHash(elem.hashCode(), elem);
        } else {
            // Resize is OK
            hashSet = hashSet.remove(elem, identityClassifer);
        }

        // The first check increases the chance of the second (more expensive one) succeeding
        if ((hashSet.getOccupiedCells() <= 13) && (size() <= 24)) {
            // The new size is at or below 24
            return new Set24<T>(getElements());
        } else {
            return this;
        }
    }

    @Override
    public Set<T> addElement(T elem) {
        // Resize is OK on add
        hashSet = hashSet.add(elem, identityClassifer);

        return this;
    }
}
