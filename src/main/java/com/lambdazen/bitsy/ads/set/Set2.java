package com.lambdazen.bitsy.ads.set;

public class Set2<T> extends PrimitiveSet<T> implements Set<T> {
    T elem0, elem1;
    
    public Set2(T elem0, T elem1) {
        this.elem0 = elem0;
        this.elem1 = elem1;
    }

    public Set2(Set3<T> oldBag) {
        this.elem0 = oldBag.elem0;
        this.elem1 = oldBag.elem1;
    }

    @Override
    public Object[] elements() {
        return new Object[] {elem0, elem1};
    }

    protected void write(int index, T elem) {
        switch (index) {
        case 0:
            elem0 = elem;
            break;
            
        case 1: 
            elem1 = elem;
            break;
            
        default: 
            throw new IllegalArgumentException("Invalid index " + index);
        }
    }

    @Override
    Set<T> expand(T elem) {
        return new Set3<T>(this, elem);
    }
    

    @Override
    int contractThreshold() {
        return 1;
    }

    @Override
    Object contract() {
        return this.elem0;
    }
}
