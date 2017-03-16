package com.lambdazen.bitsy.ads.set;

public class Set24<T> extends PrimitiveSet<T> implements Set<T> {
    T elem0, elem1, elem2, elem3, elem4, elem5, elem6, elem7, elem8, elem9, elem10, elem11,
      elem12, elem13, elem14, elem15, elem16, elem17, elem18, elem19, elem20, elem21, elem22, elem23;
    
    public Set24(Set12<T> oldSet, T elem) {
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
        this.elem12 = elem;
    }

    public Set24(Object[] elements) {
        this.elem0 = lookup(elements, 0);
        this.elem1 = lookup(elements, 1);
        this.elem2 = lookup(elements, 2);
        this.elem3 = lookup(elements, 3);
        this.elem4 = lookup(elements, 4);
        this.elem5 = lookup(elements, 5);
        this.elem6 = lookup(elements, 6);
        this.elem7 = lookup(elements, 7);
        this.elem8 = lookup(elements, 8);
        this.elem9 = lookup(elements, 9);
        this.elem10 = lookup(elements, 10);
        this.elem11 = lookup(elements, 11);
        this.elem12 = lookup(elements, 12);
        this.elem13 = lookup(elements, 13);
        this.elem14 = lookup(elements, 14);
        this.elem15 = lookup(elements, 15);
        this.elem16 = lookup(elements, 16);
        this.elem17 = lookup(elements, 17);
        this.elem18 = lookup(elements, 18);
        this.elem19 = lookup(elements, 19);
        this.elem20 = lookup(elements, 20);
        this.elem21 = lookup(elements, 21);
        this.elem22 = lookup(elements, 22);
        this.elem23 = lookup(elements, 23);
    }

    private T lookup(Object[] arr, int index) {
        return (index < arr.length) ? (T)arr[index] : null;
    }
    @Override
    public Object[] elements() {
        return new Object[] {elem0, elem1, elem2, elem3, elem4, elem5, elem6, elem7, elem8, elem9, elem10, elem11,
                elem12, elem13, elem14, elem15, elem16, elem17, elem18, elem19, elem20, elem21, elem22, elem23};
    }

    protected void write(int index, T elem) {
        if (index < 12) {
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
        } else {
            if (index < 16) {
                switch (index) {
                case 12:
                    this.elem12 = elem;
                    break;
                    
                case 13: 
                    this.elem13 = elem;
                    break;
        
                case 14: 
                    this.elem14 = elem;
                    break;
                    
                case 15:  
                    this.elem15 = elem;
                    break;
                    
                default: 
                    throw new RuntimeException("Bug in code");
                }
            } else if (index < 20) {
                switch (index) {            
                case 16:
                    this.elem16 = elem;
                    break;
    
                case 17: 
                    this.elem17 = elem;
                    break;
    
                case 18: 
                    this.elem18 = elem;
                    break;
    
                case 19: 
                    this.elem19 = elem;
                    break;
                    
                default:
                    throw new IllegalArgumentException("Invalid index " + index);
                }
            } else {
                switch (index) {            
                case 20:
                    this.elem20 = elem;
                    break;
    
                case 21: 
                    this.elem21 = elem;
                    break;
    
                case 22: 
                    this.elem22 = elem;
                    break;
    
                case 23: 
                    this.elem23 = elem;
                    break;
                    
                default:
                    throw new IllegalArgumentException("Invalid index " + index);
                }
            }
        }
    }

    @Override
    Set<T> expand(T elem) {
        return new SetMax<T>(this, elem);
    }

    @Override
    int contractThreshold() {
        return 12;
    }

    @Override
    Set<T> contract() {
        return new Set12<T>(this);
    }
}
