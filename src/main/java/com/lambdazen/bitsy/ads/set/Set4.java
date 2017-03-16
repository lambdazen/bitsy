package com.lambdazen.bitsy.ads.set;

public class Set4<T> extends PrimitiveSet<T> implements Set<T> {
    T elem0, elem1, elem2, elem3;
    
    public Set4(Set3<T> oldSet, T elem) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
        this.elem3 = elem;
    }

    public Set4(Set6<T> oldSet) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
        this.elem3 = oldSet.elem3;
    }

    @Override
    public Object[] elements() {
        return new Object[] {elem0, elem1, elem2, elem3};
    }

    protected void write(int index, T elem) {
        switch (index) {
        case 0:
            elem0 = elem;
            break;

        case 1: 
            elem1 = elem;
            break;

        case 2: 
            elem2 = elem;
            break;

        case 3: 
            elem3 = elem;
            break;

        default: 
            throw new IllegalArgumentException("Invalid index " + index);
        }
    }

    @Override
    Set<T> expand(T elem) {
        return new Set6<T>(this, elem);
    }
    

    @Override
    int contractThreshold() {
        return 3;
    }

    @Override
    Set<T> contract() {
        return new Set3<T>(this);
    }
}
