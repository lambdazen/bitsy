package com.lambdazen.bitsy.util;

import java.util.List;
import junit.framework.TestCase;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.util.DoubleBuffer.BufferName;

public class DoubleBufferTest extends TestCase {
    // Temp vars used in inner classes
    int count;
    int potFn;
    
    public DoubleBufferTest() {
        
    }
    
    public void testFixedCtBuf() throws Exception {
        Thread.sleep(1000);
        int initThreadCount = Thread.activeCount();

        final int flushSlowSleep = 50;
        for (boolean flushSlow : new boolean[] {false, true}) {
            final boolean slow = flushSlow;
            for (int threshold : new int[] {1, 5, 20, 100}) {
                System.out.println("Testing fixed count buffer with slow " + slow + ", and threshold " + threshold);
                final int finalThreshold = threshold;

                potFn = 0;
                count = 0;
                DoubleBuffer<Integer> buf = new DoubleBuffer<Integer>(new BufferPotential<Integer>() {
                    @Override
                    public boolean addWork(Integer newWork) {
                        potFn += newWork.intValue();
                        return (potFn >= finalThreshold);
                    }

                    @Override
                    public void reset() {
                        potFn = 0;
                    }

                }, new BufferFlusher<Integer>() {
                    BufferName oldBufName = BufferName.B;

                    @Override
                    public void flushBuffer(BufferName bufName, List<Integer> workList) throws BitsyException, InterruptedException {
                        if (bufName == oldBufName) {
                            throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Buffer doesn't swap");
                        }
                        
                        for (Integer work : workList) {
                            assertEquals(1, work.intValue());
                        }

                        oldBufName = bufName;
                        count++;
                        
                        if (slow) {
                            try {
                                Thread.sleep(flushSlowSleep);
                            } catch (InterruptedException e) {
                                System.out.println("Interrupted the flusher");
                                throw e;
                            }
                        }
                    }
                }, "TestThread");

                for (int i=0; i < 100; i++) {
                    buf.addWork(new Integer(1));
                    Thread.sleep(50);
                    if (slow && (i % threshold == 0)) {
                        // Sleep a little for the flusher to catch up
                        Thread.sleep(flushSlowSleep + 100);                        
                    }
                }

                assertEquals(1 + initThreadCount, Thread.activeCount());

                assertEquals(100 / finalThreshold, count);

                buf.stop(100);

                assertEquals(initThreadCount, Thread.activeCount());
            }
        }
    }
    
    public void testException() throws Exception {
        int initThreadCount = Thread.activeCount();

        System.out.println("Testing exception");

        for (boolean syncMode : new boolean[] {false, true}) {
            System.out.println("Sync mode " + syncMode);

            DoubleBuffer<Integer> buf = new DoubleBuffer<Integer>(new BufferPotential<Integer>() {
                @Override
                public boolean addWork(Integer newWork) {
                    return true;
                }

                @Override
                public void reset() {
                }

            }, new BufferFlusher<Integer>() {
                BufferName oldBufName = BufferName.B;

                @Override
                public void flushBuffer(BufferName bufName, List<Integer> workList) throws BitsyException, InterruptedException {
                    assertNull(workList);

                    if (bufName == oldBufName) {
                        throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Buffer doesn't swap");
                    }

                    Thread.sleep(2000); // wait 2 seconds

                    if (bufName == BufferName.A) {
                        throw new BitsyException(BitsyErrorCodes.ACCESS_OUTSIDE_TX_SCOPE, "test");
                    }

                    oldBufName = bufName;
                    count++;
                }
            }, "TestThread", 
            false, 
            true); // sync mode is true

            buf.addWork(new Integer(1));
            
            if (syncMode) {
                Thread.sleep(500); // wait 0.5 seconds... because of sync mode addMode will have to wait
            } else {
                Thread.sleep(3000); // wait till the flush is done
            }

            try {
                // If sync mode weren't set, add work would have gone through
                long ts = System.currentTimeMillis();
                buf.addWork(new Integer(2));
                
                if (syncMode) {
                    // Sync mode may require another addwork for the exception to register
                    Thread.sleep(100);
                    buf.addWork(new Integer(3));
                }
                
                assertEquals(syncMode, System.currentTimeMillis() - ts > 1000); // with sync mode at least 1 second must pass before, addWork gets through. 

                fail("Needs to throw an exception");
            } catch (BitsyException e) {
                assertEquals(BitsyErrorCodes.EXCEPTION_IN_FLUSH, e.getErrorCode());
                BitsyException cause = (BitsyException)e.getCause();
                assertEquals(BitsyErrorCodes.ACCESS_OUTSIDE_TX_SCOPE, cause.getErrorCode());
                assertTrue(cause.getMessage().contains("test"));
            }

            // Flush thread must have died already
            Thread.sleep(100);
            //assertEquals(initThreadCount, Thread.activeCount());

            buf.stop(1000);
            Thread.sleep(100);

            //assertEquals(initThreadCount, Thread.activeCount());
        }
    }
}
