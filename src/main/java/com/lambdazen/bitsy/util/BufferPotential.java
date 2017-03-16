package com.lambdazen.bitsy.util;

/**
 * This interface represents a buffer potential that will be updated on each
 * enqueue. The implementing class need not be thread-safe, but the DoubleBuffer
 * will explicitly synchronize on this object before calling addWork or reset().
 * The implementing class can have other synchronized methods to reconfigure
 * itself
 */
public interface BufferPotential<T> {
    /**
     * This method is invoked on each enqueue with the additional work
     * potential. If the total work reaches a threshold, it will return true.
     * Otherwise, it can return false.
     */
    public boolean addWork(T newWork);
    
    /**
     * This method is called to reset the potential, when the double buffer
     * flips the enqueue buffer
     */
    public void reset();
}
