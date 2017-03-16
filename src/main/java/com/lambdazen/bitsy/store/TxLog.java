package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.util.CommittableFileLog;

/**
 * This class captures a transaction log that needs to be merged into the VA/B and EA/B text files
 */
public class TxLog implements IVeReorgJob {
    int rpd;
    CommittableFileLog cfl;

    public TxLog(CommittableFileLog cfl) {
        this.cfl = cfl;
        this.rpd = 0;
    }

    public CommittableFileLog getCommittableFileLog() {
        return cfl;
    }
    
    public void setReorgPotDiff(int rpd) {
        this.rpd = rpd;
    }
    
    public int getReorgPotDiff() {
        return rpd;
    }
}
