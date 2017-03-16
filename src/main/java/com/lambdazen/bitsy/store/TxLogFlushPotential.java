package com.lambdazen.bitsy.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdazen.bitsy.store.FileBackedMemoryGraphStore.FlushNowJob;
import com.lambdazen.bitsy.util.BufferPotential;

/**
 * This potential function keeps track of the total bytes written to TA or
 * TB.txt and suggests a flush operation when that number exceeds the given
 * txLogThreshold
 */
public class TxLogFlushPotential implements BufferPotential<ITxBatchJob> {
    private static final Logger log = LoggerFactory.getLogger(TxLogFlushPotential.class);
    
    long txLogThreshold;
    long curBufSize;
    
    public TxLogFlushPotential(long txLogThreshold) {
        this.txLogThreshold = txLogThreshold;
        this.curBufSize = 0;
    }
    
    
    public long getTxLogThreshold() {
        return txLogThreshold;
    }

    public void setTxLogThreshold(long txLogThreshold) {
        this.txLogThreshold = txLogThreshold;
    }

    @Override
    public boolean addWork(ITxBatchJob newWork) {
        if (newWork instanceof TxBatch) {
            curBufSize += ((TxBatch)newWork).getSize();

            return (curBufSize > txLogThreshold);
        } else if (newWork instanceof FlushNowJob) {
            log.debug("Tx buffer has been flushed explicitly");

            return true;
        } else {
            // Error has already been logged before
            log.debug("Unsupported type of work in TxLogFlushPotential: {}", newWork.getClass());
            
            return false;
        }
    }

    @Override
    public void reset() {
        curBufSize = 0;
    }
}
