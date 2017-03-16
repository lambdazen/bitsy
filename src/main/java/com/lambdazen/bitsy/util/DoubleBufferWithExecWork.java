package com.lambdazen.bitsy.util;

import com.lambdazen.bitsy.BitsyException;

public class DoubleBufferWithExecWork<T> extends DoubleBuffer<T> {
    BufferQueuer<T> queuer;
    
    public DoubleBufferWithExecWork(BufferPotential<T> initPot, BufferQueuer<T> queuer, BufferFlusher<T> flusher, String flushThreadName, boolean trackWork, boolean syncMode, BufferName initBuffer) {
        super(initPot, flusher, flushThreadName, trackWork, syncMode);
        this.queuer = queuer;

        // The starting buffer is based on initBuffer 
        this.enqueueIdx = (initBuffer == BufferName.A) ? 0 : 1;
    }
    
    public void addAndExecuteWork(final T work) throws BitsyException {
        synchronized (pot) {
            // Do the work inside the synchronized block so that a buffer
            // doesn't get swapped out in the middle
            queuer.onQueue(getEnqueueBuffer(), work);

            // The work object may be modified in the previous step
            addWork(work);            
        }
    }
}
