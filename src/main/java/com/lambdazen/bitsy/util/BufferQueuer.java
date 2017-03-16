package com.lambdazen.bitsy.util;

import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.util.DoubleBuffer.BufferName;

/** This interface represents a flush worker that takes can empty a buffer (A/B) */
public interface BufferQueuer<T> {
    // Any exception thrown by this method will stop further enqueues.
    // InterruptedExceptions must be rethrown to kill the flush thread
    public void onQueue(BufferName bufName, T work) throws BitsyException; 
}
