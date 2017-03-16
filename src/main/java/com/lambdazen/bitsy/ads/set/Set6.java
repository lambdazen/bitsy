package com.lambdazen.bitsy.ads.set;

public class Set6<T> extends PrimitiveSet<T> implements Set<T> {
    T elem0, elem1, elem2, elem3, elem4, elem5;
    
    public Set6(Set4<T> oldSet, T elem) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
        this.elem3 = oldSet.elem3;
        this.elem4 = elem;
    }

    public Set6(Set8<T> oldSet) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
        this.elem3 = oldSet.elem3;
        this.elem4 = oldSet.elem4;
        this.elem5 = oldSet.elem5;
    }

    @Override
    public Object[] elements() {
        return new Object[] {elem0, elem1, elem2, elem3, elem4, elem5};
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

        case 4: 
            elem4 = elem;
            break;

        case 5: 
            elem5 = elem;
            break;

        default: 
            throw new IllegalArgumentException("Invalid index " + index);
        }
    }

    @Override
    Set<T> expand(T elem) {
        return new Set8<T>(this, elem);
    }
    

    @Override
    int contractThreshold() {
        return 4;
    }

    @Override
    Set<T> contract() {
        return new Set4<T>(this);
    }
}
