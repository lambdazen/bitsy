package com.lambdazen.bitsy.store;

import java.util.List;

/**
 * This class captures a list of transactions that must be written to the
 * VA/B and EA/B text files
 */
public class TxBatch implements ITxBatchJob {
    List<TxUnit> trans;
    int size;

    public TxBatch(List<TxUnit> trans) {
        this.trans = trans;
        this.size = 0;
    }

    public List<TxUnit> getTxUnitList() {
        return trans;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}
