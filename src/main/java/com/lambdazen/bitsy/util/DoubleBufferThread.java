package com.lambdazen.bitsy.util;

import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.util.DoubleBuffer.BufferName;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class flushes the double buffer based on the potential function provided to it */
public class DoubleBufferThread<T> extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DoubleBuffer.class);

    DoubleBuffer<T> buf;
    BufferFlusher<T> flush;
    boolean syncMode;
    boolean stopped;

    public DoubleBufferThread(String threadName, DoubleBuffer<T> buf, BufferFlusher<T> flush, boolean syncMode) {
        super(threadName);
        setDaemon(true);

        this.buf = buf;
        this.flush = flush;
        this.syncMode = syncMode;
        this.stopped = false;
    }

    public void safeStop() {
        this.stopped = true;
    }

    public void run() {
        try {
            while (!stopped) {
                if (syncMode) {
                    synchronized (buf.getPotential()) {
                        doFlush();
                    }
                } else {
                    doFlush();
                }
            }
        } catch (InterruptedException e) {
            // Exiting thread
            log.error(
                    Thread.currentThread().getName()
                            + " was interrupted, most likely because a safe stop was not possible. This may result in recovery-related warnings during the next startup",
                    e);
        }
    }

    public void doFlush() throws InterruptedException {
        BufferName bufToFlush = buf.getBufferToFlush();

        if (stopped) {
            // Exit
            return;
        }

        List<T> workList = buf.getWorkList(bufToFlush);

        // Invoke the new flusher on the dequeue buffer
        try {
            flush.flushBuffer(bufToFlush, workList);
        } catch (BitsyException e) {
            BitsyException bitsyException = new BitsyException(
                    BitsyErrorCodes.EXCEPTION_IN_FLUSH, "Encountered exception in thread " + getName(), e);
            buf.setException(bitsyException);
            log.error(getName() + " encountered an unrecoverable exception", bitsyException);

            // Exit to avoid completing the flush. The next time may have a chance.
            return;
        }

        // Don't flush the next time -- till the potential function triggers
        buf.completedFlush();
    }
}
