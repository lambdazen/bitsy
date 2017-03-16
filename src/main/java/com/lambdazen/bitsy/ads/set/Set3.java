package com.lambdazen.bitsy.ads.set;

public class Set3<T> extends PrimitiveSet<T> implements Set<T> {
    T elem0, elem1, elem2;
    
    public Set3(Set2<T> oldSet, T elem) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = elem;
    }

    public Set3(Set4<T> oldSet) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
    }

    @Override
    public Object[] elements() {
        return new Object[] {elem0, elem1, elem2};
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

        default: 
            throw new IllegalArgumentException("Invalid index " + index);
        }
    }

    @Override
    Set<T> expand(T elem) {
        return new Set4<T>(this, elem);
    }
    

    @Override
    int contractThreshold() {
        return 2;
    }

    @Override
    Set<T> contract() {
        return new Set2<T>(this);
    }
}
