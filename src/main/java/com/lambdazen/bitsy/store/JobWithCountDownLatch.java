package com.lambdazen.bitsy.store;

import java.util.concurrent.CountDownLatch;

public class JobWithCountDownLatch {
    CountDownLatch cdl;

    public JobWithCountDownLatch() {
        this.cdl = new CountDownLatch(1);
    }

    public CountDownLatch getCountDownLatch() {
        return cdl;
    }
}
