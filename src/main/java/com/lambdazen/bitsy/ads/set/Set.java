package com.lambdazen.bitsy.ads.set;

/** A bag keeps an unordered, unstable collection of elements */ 
public interface Set<T> {
    public int size();

    public Object[] getElements();

    public Object removeElement(T elem);

    public Set<T> addElement(T elem);
}
