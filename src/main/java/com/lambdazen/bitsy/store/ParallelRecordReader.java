package com.lambdazen.bitsy.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.util.CommittableFileLog;

public class ParallelRecordReader extends RecordReader {
    private static final Logger log = LoggerFactory.getLogger(ParallelRecordReader.class);
    
    public static int QUEUE_TO_PROCESSOR_RATIO = 3;
    
    private int numLinesPerBatch;
    private int numBatchInQueue;
    private int numProcessors;

    private ExecutorService producerService;
    private ExecutorService deserializerService;
    private ArrayBlockingQueue<Batch> queue;
    private boolean isDone = false;

    private boolean stopped = false;

    private Iterator<Record> currentIterator;
    
    public ParallelRecordReader(CommittableFileLog cfl, int numLinesPerBatch, ObjectReader vReader, ObjectReader eReader) {
        super(cfl, vReader, eReader);
        
        this.numLinesPerBatch = numLinesPerBatch;
        
        this.numProcessors = Runtime.getRuntime().availableProcessors() - 1; // one to insert, read is not 100% busy
        if (numProcessors < 1) {
            this.numProcessors = 1;
        } else if (numProcessors > 4) {
            this.numProcessors = 4; // Don't need more than 4 threads
        }
        
        this.numBatchInQueue = QUEUE_TO_PROCESSOR_RATIO * numProcessors;
        
        this.producerService = Executors.newSingleThreadExecutor();
        this.deserializerService = Executors.newFixedThreadPool(numProcessors);
        this.queue = new ArrayBlockingQueue<Batch>(numBatchInQueue);

        // This keeps filling up the queue
        producerService.submit(new ProducerTask());
    }
    
    public Record next() throws Exception {
        if ((currentIterator == null) || (!currentIterator.hasNext())) {
            // The current iterator is done. Need to get the next iterator
            if (isDone) {
                // Shutdown remaining services
                log.debug("Shutting down services");
                shutdownServices();
                
                return null;
            }
            
            // There should be more in the buffer
            Batch nextBatch = queue.take();

            if (nextBatch.isLastBatch() || (nextBatch.getException() != null)) {
                // This boolean won't be used till the iterator is drained out
                isDone = true;
                
                if (nextBatch.getException() != null) {
                    throw nextBatch.getException();
                }
            }
            
            // Get the records, waiting for serialization if necessary
            currentIterator = nextBatch.getRecords().iterator();
        }
        
        if (currentIterator.hasNext()) {
            return currentIterator.next();
        } else {
            shutdownServices();
            return null;
        }
    }

    // Shutdown services
    private void shutdownServices() {
        if (producerService != null) {
            producerService.shutdown();
            producerService = null; 
        }
        
        if (deserializerService != null) {
            deserializerService.shutdown();
            deserializerService = null;
        }
    }
    
    public class Batch {
        List<String> lines = new ArrayList<String>(numBatchInQueue);
        List<Record> records = new ArrayList<Record>(numBatchInQueue);
        boolean lastBatch = false;
        CountDownLatch cdl = new CountDownLatch(1);
        Exception exception;
        
        public Batch() {
            try {
                log.debug("Reading a new batch from {}", cfl.getPath());
                int count = 0;
                String line;
                while ((line = cfl.readLine()) != null) {
                    count++;
                    lines.add(line);

                    //log.debug("Read line: {}", line);

                    if (count >= numLinesPerBatch) {
                        return;
                    }
                }

                log.debug("Reached end of {}", cfl.getPath());
                lastBatch = true;
            } catch (BitsyException e) {
                // Stop parsing
                exception = e;
            }
        }
        
        public boolean isLastBatch() {
            return lastBatch;
        }
        
        public void deserialize() throws JsonProcessingException, IOException {
            try {
                if (exception == null) {
                    // De-serialize only if the read was successful
                    log.debug("Deserializing batch from {}", cfl.getPath());
                    for (String line : lines) {
                        Record rec = Record.parseRecord(line, lineNo++, fileName);
                        rec.deserialize(vReader, eReader);
                        records.add(rec);
                    }
                }
            } finally {
                // Don't hold up the next step irrespective of the exception  
                cdl.countDown();
            }
        }
        
        public List<Record> getRecords() throws InterruptedException {
            // Wait if serialization is not complete
            cdl.await();
            
            return records; 
        }

        public void setException(Exception e) {
            this.exception = e;
        }
        
        public Exception getException() {
            return exception;
        }
    }
    
    public class ProducerTask implements Runnable {
        public void run() {
            while (!stopped) {
                final Batch batch = new Batch();
            
                // Add batch to the queue
                try {
                    queue.put(batch);
                } catch (InterruptedException e) {
                    log.error("Producer task has been interrupted", e);
                    return;
                }
                
                // Schedule the serializer
                deserializerService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            batch.deserialize();
                        } catch (Exception e) {
                            batch.setException(e);
                        }
                    }
                });
                
                // Are we done yet?
                if (batch.isLastBatch()) {
                    // Producer's work is done
                    return;
                }
            }
        }
    }
}
