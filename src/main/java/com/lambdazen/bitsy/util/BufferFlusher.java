package com.lambdazen.bitsy.util;

import java.util.List;

import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.util.DoubleBuffer.BufferName;

/** This interface represents a flush worker that takes can empty a buffer (A/B) */
public interface BufferFlusher<T> {
    // Any exception thrown by this method will stop further enqueues.
    // InterruptedExceptions must be rethrown to kill the flush thread
    public void flushBuffer(BufferName bufName, List<T> workList) throws BitsyException, InterruptedException; 
}
