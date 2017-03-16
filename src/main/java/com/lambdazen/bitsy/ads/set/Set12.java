package com.lambdazen.bitsy.ads.set;

public class Set12<T> extends PrimitiveSet<T> implements Set<T> {
    T elem0, elem1, elem2, elem3, elem4, elem5, elem6, elem7, elem8, elem9, elem10, elem11;
    
    public Set12(Set8<T> oldSet, T elem) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
        this.elem3 = oldSet.elem3;
        this.elem4 = oldSet.elem4;
        this.elem5 = oldSet.elem5;
        this.elem6 = oldSet.elem6;
        this.elem7 = oldSet.elem7;
        this.elem8 = elem;
    }

    public Set12(Set24<T> oldSet) {
        this.elem0 = oldSet.elem0;
        this.elem1 = oldSet.elem1;
        this.elem2 = oldSet.elem2;
        this.elem3 = oldSet.elem3;
        this.elem4 = oldSet.elem4;
        this.elem5 = oldSet.elem5;
        this.elem6 = oldSet.elem6;
        this.elem7 = oldSet.elem7;
        this.elem8 = oldSet.elem8;
        this.elem9 = oldSet.elem9;
        this.elem10 = oldSet.elem10;
        this.elem11 = oldSet.elem11;
    }

    @Override
    public Object[] elements() {
        return new Object[] {elem0, elem1, elem2, elem3, elem4, elem5, elem6, elem7, elem8, elem9, elem10, elem11};
    }

    protected void write(int index, T elem) {
        if (index < 4) {
            switch (index) {
            case 0:
                this.elem0 = elem;
                break;
                
            case 1: 
                this.elem1 = elem;
                break;
    
            case 2: 
                this.elem2 = elem;
                break;
                
            case 3: 
                this.elem3 = elem;
                break;
                
            default: 
                throw new RuntimeException("Bug in code");
            }
        } else if (index < 8) {
            switch (index) {            
            case 4:
                this.elem4 = elem;
                break;

            case 5: 
                this.elem5 = elem;
                break;

            case 6: 
                this.elem6 = elem;
                break;

            case 7: 
                this.elem7 = elem;
                break;
                
            default:
                throw new IllegalArgumentException("Invalid index " + index);
            }
        } else {
            switch (index) {            
            case 8:
                this.elem8 = elem;
                break;

            case 9: 
                this.elem9 = elem;
                break;

            case 10: 
                this.elem10 = elem;
                break;

            case 11: 
                this.elem11 = elem;
                break;
                
            default:
                throw new IllegalArgumentException("Invalid index " + index);
            }
        }
    }

    @Override
    Set<T> expand(T elem) {
        return new Set24<T>(this, elem);
    }

    @Override
    int contractThreshold() {
        return 8;
    }

    @Override
    Set<T> contract() {
        return new Set8<T>(this);
    }
}
