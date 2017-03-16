package com.lambdazen.bitsy.util;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdazen.bitsy.BitsyException;

/**
 * This class implements a double buffer that lets one set of Threads enqueue
 * work on one queue while a "flush thread" performs the work queued up on the
 * other queue
 */ 
public class DoubleBuffer<T> {
    private static final Logger log = LoggerFactory.getLogger(DoubleBuffer.class);
    
    public enum BufferName {A, B};
    
    // State of the double buffer
    int enqueueIdx;
    boolean[] needFlush;
    List<T> workListA;
    List<T> workListB;
    
    BitsyException toThrow;
    boolean trackWork;

    // Helper objects
    BufferPotential<T> pot;
    DoubleBufferThread<T> flushThread;
    
    
    /**
     * This constructor takes an executor service on which the FlushWorker will
     * be called when it is time to flush the buffer. 
     */
    public DoubleBuffer(BufferPotential<T> initPot, BufferFlusher<T> flusher, String flushThreadName) {
        this(initPot, flusher, flushThreadName, true, false);
    }
    
    public DoubleBuffer(BufferPotential<T> initPot, BufferFlusher<T> flusher, String flushThreadName, boolean trackWork, boolean syncMode) {
        this.enqueueIdx = 0;
        
        this.pot = initPot;
        this.needFlush = new boolean[] {false, false};
        this.flushThread = new DoubleBufferThread<T>(flushThreadName, this, flusher, syncMode);
        this.trackWork = trackWork;
        
        if (trackWork) {
            this.workListA = new ArrayList<T>();
            this.workListB = new ArrayList<T>();
        }
        
        flushThread.start();
    }
    
    public BufferPotential<T> getPot() {
        return pot;
    }

    public void stop(int joinTimeout) {
        if (flushThread != null) {
            synchronized (pot) {
                needFlush[0] = true;
                needFlush[1] = true;
                pot.notifyAll();
                flushThread.safeStop();
            }
            
            try {
                flushThread.join(joinTimeout);

                flushThread.interrupt();
                flushThread.join(joinTimeout);
            } catch (InterruptedException e) {
                // Some other thread interrupted this one
                log.error(Thread.currentThread().getName() + " was interrupted during stop() by a different thread", e);
            }
            
            flushThread = null;
        }
    }
    
    public void addWork(final T work) throws BitsyException {
        synchronized (pot) {
            // There is actual work being enqueued by a thread
            if (toThrow != null) {
                throw toThrow;
            }

            if (trackWork) {
                (enqueueIdx == 0 ? workListA : workListB).add(work);
            }
            
            boolean needFlushThisTime = pot.addWork(work);
            this.needFlush[enqueueIdx] = needFlush[enqueueIdx] || needFlushThisTime;

            if (needFlushThisTime) {
                pot.notifyAll();
            }
        }
    }
    
    public BufferName getBufferToFlush() throws InterruptedException {
        synchronized (pot) {
            // Flush if need to flush is true on the enqueue index 
            while (!needFlush[enqueueIdx]) {
                pot.wait();
            }
            
            // Flush buffer is now the enqueue index
            BufferName flushBuf = getEnqueueBuffer();
            
            // The enqueue buffer moves to the other buffer
            enqueueIdx = 1 - enqueueIdx;
            
            // Now that we have moved to the other queue, the potential function can be reset
            pot.reset();
            
            return flushBuf;
        }
    }
    
    public List<T> getWorkList(BufferName bufName) {
        // This method will return null if the object was initialized with trackWork = false 
        return (bufName == BufferName.A) ? workListA : workListB;
    }
    
    public BufferName getEnqueueBuffer() {
        return (enqueueIdx == 0) ? BufferName.A : BufferName.B;
    }
    
    public void completedFlush() {
        synchronized (pot) {
            // Done with the flush on the 'other' queue 
            needFlush[1 - enqueueIdx] = false;
            
            // Clear the work list
            if (trackWork) {
                ((enqueueIdx == 0) ? workListB : workListA).clear();
            }
        }
    }

    public void setException(BitsyException bitsyException) {
        synchronized (pot) {
            toThrow = bitsyException;
        }
    }

    public BufferPotential<T> getPotential() {
        return pot;
    }
}
