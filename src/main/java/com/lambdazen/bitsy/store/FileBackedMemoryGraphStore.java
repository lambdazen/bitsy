package com.lambdazen.bitsy.store;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.ICommitChanges;
import com.lambdazen.bitsy.IGraphStore;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.store.Record.RecordType;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import com.lambdazen.bitsy.util.BufferFlusher;
import com.lambdazen.bitsy.util.BufferPotential;
import com.lambdazen.bitsy.util.BufferQueuer;
import com.lambdazen.bitsy.util.CommittableFileLog;
import com.lambdazen.bitsy.util.DoubleBuffer;
import com.lambdazen.bitsy.util.DoubleBuffer.BufferName;
import com.lambdazen.bitsy.util.DoubleBufferWithExecWork;

/** This class represents a memory graph store that is backed by a durable files */
public class FileBackedMemoryGraphStore implements IGraphStore {
	private static final Logger log = LoggerFactory.getLogger(FileBackedMemoryGraphStore.class);

    private static final String META_PREFIX = "meta";
    private static final String META_B_TXT = "metaB.txt";
    private static final String META_A_TXT = "metaA.txt";
    private static final String E_B_TXT = "eB.txt";
    private static final String E_A_TXT = "eA.txt";
    private static final String V_B_TXT = "vB.txt";
    private static final String V_A_TXT = "vA.txt";
    private static final String TX_B_TXT = "txB.txt";
    private static final String TX_A_TXT = "txA.txt";
    
    // Commit 10K ops per load in the V/E files
    public static final int DEFAULT_LOAD_OPS_PER_COMMIT = 10000;
    public static final int DEFAULT_MIN_LINES_BEFORE_REORG = 1000;
    
    public static final Random rand = new Random();
    
    public static final Charset utf8 = Charset.forName("utf-8");
    private static final int JOIN_TIMEOUT = 60000; // 1 minute

    private static AtomicInteger idCounter = new AtomicInteger(1);
    private static AtomicBoolean backupInProgress = new AtomicBoolean(false);
    
    private int id; 
    private ObjectMapper mapper;
    private MemoryGraphStore memStore;
    private Path dbPath;

    private CommittableFileLog txA;
    private CommittableFileLog txB;
    private CommittableFileLog vA;
    private CommittableFileLog vB;
    private CommittableFileLog eA;
    private CommittableFileLog eB;
    private CommittableFileLog mA;
    private CommittableFileLog mB;
    
    private DoubleBuffer<TxUnit> txToTxLogBuf;
    private DoubleBufferWithExecWork<ITxBatchJob> txLogToVEBuf;
    private DoubleBufferWithExecWork<IVeReorgJob> veReorgBuf;
    
    private TxLogFlushPotential txLogFlushPotential;
    private VEObsolescencePotential veReorgPotential;
    
    private long logCounter;

    private BufferName lastFlushedBuffer = null;
    private Object flushCompleteSignal = new Object(); 
    
    // Major version number
    public static String CURRENT_MAJOR_VERSION_NUMBER = "1.5"; 
    private String majorVersionNumber = "1.0";

    public FileBackedMemoryGraphStore(MemoryGraphStore memStore, Path dbPath, long txLogThreshold, double reorgFactor) {
    	this(memStore, dbPath, txLogThreshold, reorgFactor, false);
    }
    
    public FileBackedMemoryGraphStore(MemoryGraphStore memStore, Path dbPath, long txLogThreshold, double reorgFactor, boolean createDirIfMissing) {
        this.id = idCounter.getAndIncrement();
        this.memStore = memStore;
        this.dbPath = dbPath;
        log.info("Starting graph " + toString());
       
        this.mapper = new ObjectMapper();
        // Indentation must be turned off
        mapper.configure(SerializationFeature.INDENT_OUTPUT, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
        mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.enableDefaultTyping();
        
        if (!dbPath.toFile().isDirectory()) {
        	if (!createDirIfMissing) {
        		throw new BitsyException(BitsyErrorCodes.BAD_DB_PATH, "Expecting " + dbPath + " to be a folder. Exists? " + dbPath.toFile().exists());
        	} else {
        		if (!dbPath.toFile().exists()) {
        			try {
        				Files.createDirectory(dbPath);
        			} catch (IOException ex) {
        				throw new BitsyException(BitsyErrorCodes.BAD_DB_PATH, "Couldn't create " + dbPath);
        			}
        		}
        	}
        }

        // Start off the Log Counter as 1. openForRead() will update this to the maximum so far.
        this.logCounter = 1;
        
        this.txA = openFileLog(TX_A_TXT, true);
        this.txB = openFileLog(TX_B_TXT, true);
        this.vA = openFileLog(V_A_TXT, false);
        this.vB = openFileLog(V_B_TXT, false);
        this.eA = openFileLog(E_A_TXT, false);
        this.eB = openFileLog(E_B_TXT, false);
        this.mA = openFileLog(META_A_TXT, false);
        this.mB = openFileLog(META_B_TXT, false);
        
        log.debug("Initial log counter is {}", logCounter);
        
        // Find the earlier of the two
        CommittableFileLog vToLoad = getEarlierBuffer(vA, vB);
        CommittableFileLog eToLoad = getEarlierBuffer(eA, eB);
        CommittableFileLog[] txToLoad = getOrderedTxLogs(txA, txB);

        List<CommittableFileLog> logsToLoad = new ArrayList<CommittableFileLog>();
        
        logsToLoad.add(vToLoad);
        logsToLoad.add(eToLoad);
        logsToLoad.addAll(Arrays.asList(txToLoad));

        // Load the records from files to the memory graphs store
        log.debug("Loading logs in this order: {}", logsToLoad);
        
        LoadTask loadTask;
        try {
            loadTask = new LoadTask(logsToLoad.toArray(new CommittableFileLog[0]), (MemoryGraphStore)memStore, DEFAULT_LOAD_OPS_PER_COMMIT, mapper, false);
            loadTask.run();
        } catch (BitsyException e) {
            // Failed -- not try in repair mode
            log.info("Loading the database failed -- Trying again in repair mode");
            memStore.reset();
            loadTask = new LoadTask(logsToLoad.toArray(new CommittableFileLog[0]), (MemoryGraphStore)memStore, DEFAULT_LOAD_OPS_PER_COMMIT, mapper, true);
            loadTask.run();
        }

        long initialVE = loadTask.getTotalVE();
        
        loadVersionAndIndexes();
        if (!majorVersionNumber.equals(CURRENT_MAJOR_VERSION_NUMBER)) {
            log.error("Can not load database with major version number {}. Expecting major version number {}", majorVersionNumber, CURRENT_MAJOR_VERSION_NUMBER);
            
            throw new BitsyException(BitsyErrorCodes.MAJOR_VERSION_MISMATCH, "Database has major version number " + majorVersionNumber + ". Expecting major version " + CURRENT_MAJOR_VERSION_NUMBER);
        }
        

        // Find out the transaction log that must be queued into first
        BufferName txBufName = (txToLoad[1] == txA) ? BufferName.A : BufferName.B;
        Long nextTxCounter = txToLoad[1].getCounter();
        assert nextTxCounter != null;

        // Find Tx file to flush
        CommittableFileLog txToFlush = txToLoad[0];

        // Set up the file channels / buffered streams
        txToFlush.openForRead();
        prepareForAppend(vToLoad);
        prepareForAppend(eToLoad);

        CompactAndCopyTask txFlushTask = new CompactAndCopyTask(new CommittableFileLog[] {txToFlush}, vToLoad, eToLoad, memStore, nextTxCounter);
        txFlushTask.run();
        if (txFlushTask.getOutputLines() > 0) {
            log.debug("Flushed partially flushed Tx Log {} to {} and {}", txToFlush, vToLoad, eToLoad);
        }
        
        // Clear the TX file 
        txToFlush.openForOverwrite(logCounter++);

        // Clear the unused V and E buffers
        CommittableFileLog vToClear = (vToLoad == vA) ? vB : vA; 
        CommittableFileLog eToClear = (eToLoad == eA) ? eB : eA;
        vToClear.openForOverwrite(null);
        eToClear.openForOverwrite(null);
        
        // Find latest V and E buffers
        BufferName vBufName = (vToLoad == vA) ? BufferName.A : BufferName.B;
        BufferName eBufName = (eToLoad == eA) ? BufferName.A : BufferName.B;
        
        // See if for some reason, the V and E files have been swapped. This could happen if a reorg happens partially
        if (vBufName != eBufName) {
            // Yes. Now the edge will flip from A to B or vice versa
            CommittableFileLog eToSave = (eToLoad == eA) ? eB : eA; 
            
            eToLoad.openForRead();
            eToSave.openForOverwrite(logCounter++);
            
            log.info("Moving out-of-sync edge file from {} to {}", eToLoad, eToSave);
            CompactAndCopyTask eCopyTask = new CompactAndCopyTask(new CommittableFileLog[] {eToLoad}, vToLoad, eToSave, memStore, nextTxCounter);
            eCopyTask.run();
            
            eToLoad.openForOverwrite(null);
            eToLoad.close();
        }
        
        this.txToTxLogBuf = new DoubleBuffer<TxUnit>(new BufferPotential<TxUnit>() {
            @Override
            public boolean addWork(TxUnit newWork) {
                return true;
            }
            
            @Override
            public void reset() {
                // Nothing to do
            }
        }, new TxUnitFlusher(), "MemToTxLogWriter-" + id);
        
        this.txLogFlushPotential = new TxLogFlushPotential(txLogThreshold);
        
        this.txLogToVEBuf = new DoubleBufferWithExecWork<ITxBatchJob>(txLogFlushPotential, 
                new TxBatchQueuer(),
                new TxBatchFlusher(), 
                "TxFlusher-" + id,
                false, // Don't keep track of the list of all Txs written to log
                false, // It is OK for the flusher and queuer to run at the same time
                txBufName); // Start enqueuing into the Tx from the last start/stop 
        
        this.veReorgPotential = new VEObsolescencePotential(DEFAULT_MIN_LINES_BEFORE_REORG, reorgFactor, initialVE);
        this.veReorgBuf = new DoubleBufferWithExecWork<IVeReorgJob>(veReorgPotential, 
                new TxLogQueuer(),
                new TxLogFlusher(), 
                "VEReorg-" + id,
                false, // Don't keep track of the entire list of TxLogs -- too much memory
                true, // Ensure that the flusher and queuer don't run at the same time
                vBufName); // Start enqueuing into the V/E file from the last start/stop 
    }
    
    public TxLogFlushPotential getTxLogFlushPotential() {
        return txLogFlushPotential;
    }

    public VEObsolescencePotential getVEReorgPotential() {
        return veReorgPotential;
    }

    private void loadVersionAndIndexes() {
        CommittableFileLog inputLog = getEarlierBuffer(mA, mB);

        try {
            inputLog.openForRead();
            String fileName = inputLog.getPath().toString();
            
            int lineNo = 1;
            String line;
            while ((line = inputLog.readLine()) != null) {
                Record rec = Record.parseRecord(line, lineNo++, fileName);
                
                if (rec.getType() == RecordType.I) {
                    IndexBean iBean = mapper.readValue(rec.getJson(), IndexBean.class);

                    memStore.createKeyIndex(iBean.getKey(), iBean.getIndexClass());
                } else if (rec.getType() == RecordType.M) {
                    this.majorVersionNumber = rec.getJson();
                } else {
                    throw new BitsyException(BitsyErrorCodes.DATABASE_IS_CORRUPT, "Only M and I records are valid in the metadata file. Found " + line + " in line number " + lineNo + " of file " + fileName);
                }
            }

            inputLog.close();
        } catch (Exception e) {
            if (e instanceof BitsyException) {
                throw (BitsyException)e;
            } else {
                throw new BitsyException(BitsyErrorCodes.DATABASE_IS_CORRUPT, "Unable to load indexes due to the given exception", e);
            }
        }
    }
    
    private void saveVersionAndIndexes() {
        CommittableFileLog oldLog = getEarlierBuffer(mA, mB);
        CommittableFileLog outputLog = (oldLog == mA) ? mB : mA;

        try {
            outputLog.openForOverwrite(logCounter++);

            // Save the version
            outputLog.append(Record.generateDBLine(RecordType.M, CURRENT_MAJOR_VERSION_NUMBER).getBytes(utf8));

            // Vertex indexes
            for (String key : memStore.getIndexedKeys(Vertex.class)) {
                IndexBean indexBean = new IndexBean(0, key);
                byte[] line = Record.generateDBLine(RecordType.I, mapper.writeValueAsString(indexBean)).getBytes(utf8);
                outputLog.append(line);
            }
            
            // Edge indexes
            for (String key : memStore.getIndexedKeys(Edge.class)) {
                IndexBean indexBean = new IndexBean(1, key);
                byte[] line = Record.generateDBLine(RecordType.I, mapper.writeValueAsString(indexBean)).getBytes(utf8);
                outputLog.append(line);
            }
            
            outputLog.commit();
            outputLog.close();
            
            oldLog.openForOverwrite(null);
            oldLog.close();
        } catch (Exception e) {
            if (e instanceof BitsyException) {
                throw (BitsyException)e;
            } else {
                throw new BitsyException(BitsyErrorCodes.DATABASE_IS_CORRUPT, "Unable to load indexes due to the given exception", e);
            }
        }
    }

    private CommittableFileLog[] getOrderedTxLogs(CommittableFileLog txLog1, CommittableFileLog txLog2) {
        assert txLog1.getCounter() != null;
        assert txLog2.getCounter() != null;
        assert txLog1.getCounter().longValue() != txLog2.getCounter().longValue();
                
        if (txLog1.getCounter().longValue() < txLog2.getCounter().longValue()) {
            return new CommittableFileLog[] {txLog1, txLog2}; 
        } else {
            return new CommittableFileLog[] {txLog2, txLog1};
        }
    }

    private CommittableFileLog getEarlierBuffer(CommittableFileLog log1, CommittableFileLog log2) {
        if (log1.getCounter() == null) {
            return log2;
        } else if (log2.getCounter() == null) {
            return log1;
        } else {
            assert log1.getCounter().longValue() != log2.getCounter().longValue();
            
            return (log1.getCounter().longValue() < log2.getCounter().longValue()) ? log1 : log2;
        }
    }

    public String toString() {
        return "FileBackedMemoryGraphStore-" + id + "(path = " + dbPath + ")";
    }
    
    public void shutdown() {
        log.info("Stopping graph {}", toString());
        this.txLogToVEBuf.stop(JOIN_TIMEOUT);
        this.veReorgBuf.stop(JOIN_TIMEOUT);
        this.txToTxLogBuf.stop(JOIN_TIMEOUT);
        
        txA.close();
        txB.close();
        vA.close();
        vB.close();
        eA.close();
        eB.close();
        mA.close();
        mB.close();

        this.memStore.shutdown(); 
    }

    private CommittableFileLog openFileLog(String fileName, boolean isTxLog) throws BitsyException {
        Path toOpen = dbPath.resolve(fileName);
        try {
            CommittableFileLog cfl = new CommittableFileLog(toOpen, isTxLog);
            
            // First check if the file exists
            if (!cfl.exists()) {
                // Otherwise create it using openForOverwrite
                cfl.openForOverwrite(logCounter++);

                // Set the version for meta files
                if (fileName.startsWith(META_PREFIX)) {
                    cfl.append(Record.generateDBLine(RecordType.M, CURRENT_MAJOR_VERSION_NUMBER).getBytes(utf8));
                }

                cfl.close();
            }
            
            // Then open for read
            cfl.openForRead();
            
            log.debug("Checking file: {} with log counter {}. Size = {}", cfl.getPath(), cfl.getCounter(), cfl.size());
            
            if ((cfl.getCounter() != null) && (cfl.getCounter().longValue() >= logCounter)) {
                this.logCounter = cfl.getCounter().longValue() + 1;
            }
            
            return cfl;
        } catch (IOException e) {
            throw new BitsyException(BitsyErrorCodes.ERROR_INITIALIZING_DB_FILES, "File: " + toOpen, e);
        }
    }
    
    private void prepareForAppend(CommittableFileLog cfl) {
        if (cfl.getCounter() == null) {
            // An empty file. Need to write the header first
            log.debug("Overwriting file: {}", cfl);
            cfl.openForOverwrite(logCounter++);
        } else {
            cfl.openForAppend();
        }
    }
    
    @Override
    public VertexBean getVertex(UUID id) {
        return memStore.getVertex(id);
    }

    @Override
    public EdgeBean getEdge(UUID id) {
        return memStore.getEdge(id);
    }

    @Override
    public BitsyVertex getBitsyVertex(BitsyTransaction tx, UUID id) {
        return memStore.getBitsyVertex(tx, id);
    }

    @Override
    public BitsyEdge getBitsyEdge(BitsyTransaction tx, UUID id) {
        return memStore.getBitsyEdge(tx, id);
    }

    @Override
    public List<EdgeBean> getEdges(UUID vertexId, Direction dir, String[] edgeLabels) {
        return memStore.getEdges(vertexId, dir, edgeLabels);
    }

    @Override
    public void commit(ICommitChanges changes) {
        if ((changes.getVertexChanges().size() == 0) && (changes.getEdgeChanges().size() == 0)) {
            return;
        }

        // Phase I: Serialize the objects to make sure that they can go into the file
        TxUnit txw;
        StringWriter lineOutput = new StringWriter(); // Reused for vertex and edge lines 
        try {
            StringWriter vWriter = new StringWriter();
            for (BitsyVertex v : changes.getVertexChanges()) {
                // Increment the version before the commit
                v.incrementVersion();
                
                VertexBeanJson vBean = v.asJsonBean();
                //vWriter.write(Record.generateDBLine(RecordType.V, mapper.writeValueAsString(vBean)));
                
                Record.generateVertexLine(lineOutput, mapper, vBean);
                vWriter.write(lineOutput.toString());
            }

            StringWriter eWriter = new StringWriter();
            for (BitsyEdge e : changes.getEdgeChanges()) {
                // Increment the version before the commit
                e.incrementVersion();

                EdgeBeanJson eBean = e.asJsonBean();
                //eWriter.write(Record.generateDBLine(RecordType.E, mapper.writeValueAsString(eBean)));
                
                Record.generateEdgeLine(lineOutput, mapper, eBean);
                eWriter.write(lineOutput.toString());
            }

            byte[] vBytes = vWriter.getBuffer().toString().getBytes(utf8);
            byte[] eBytes = eWriter.getBuffer().toString().getBytes(utf8);
            
            // Transaction boundary. Has a random integer and its hashcode to verify end of Tx. 
            byte[] tBytes = Record.generateDBLine(RecordType.T, "" + rand.nextInt()).getBytes(utf8);

            txw = new TxUnit(ByteBuffer.wrap(vBytes), ByteBuffer.wrap(eBytes), ByteBuffer.wrap(tBytes));
        } catch (JsonProcessingException e) {
            throw new BitsyException(BitsyErrorCodes.SERIALIZATION_ERROR, "Encountered error", e);
        } catch (IOException e) {
            throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Unable to serialize to StringBuffer", e);
        }
        
        // Phase II: Update the memory store and push the commits to the double
        // buffer. The write-lock inside the commit() is active during the call to 
        // add the transaction to the buffer. This ensures that the transactions 
        // are written in the same order as they enter the memory store.  
        final TxUnit txwf = txw;

        // Note that the memory store reject the transaction by throwing an exception, such as BitsyRetryException
        memStore.commit(changes, false, new Runnable() {
            @Override
            public void run() {
                txToTxLogBuf.addWork(txwf);
            }
        });

        // Phase III: Push the commits through
        try {
            txw.getCountDownLatch().await();
        } catch (InterruptedException e) {
            BitsyException toThrow = new BitsyException(BitsyErrorCodes.TRANSACTION_INTERRUPTED, "Exception while waiting for transaction log to commit", e);
            
            log.error("Error while committing transaction", toThrow);
            
            throw toThrow;
        }
        
        BitsyException toThrow = txw.getException();
        if (toThrow != null) {
            throw toThrow;
        }
    }
    
    /** This method flushes the transaction log to the V/E text files */
    public void flushTxLog() {
        synchronized (flushCompleteSignal) {
            BufferName enqueueBuffer;
            synchronized (txLogToVEBuf.getPot()) {
                // Enqueue the backup task
                enqueueBuffer = txLogToVEBuf.getEnqueueBuffer(); 
                FlushNowJob flushJob = new FlushNowJob();
                txLogToVEBuf.addAndExecuteWork(flushJob);
            }
            
            try {
                do {
                    log.debug("Waiting for flush to complete in buffer {}", enqueueBuffer);
                    flushCompleteSignal.wait();
                    log.debug("Flush complete in buffer {}", lastFlushedBuffer);
                } while (lastFlushedBuffer != enqueueBuffer);
            } catch (InterruptedException e) {
                BitsyException toThrow = new BitsyException(BitsyErrorCodes.FLUSH_INTERRUPTED, "Exception while waiting for a transaction-log flush to be performed", e);

                log.error("Error while flushing the transaction log", toThrow);

                throw toThrow;
            }
        }        
    }
    
    /** This method backs up the database while it is still operational. Only one backup can be in progress at a time. 
     * 
     * @param backupDir directory to which the database must be backed up. 
     */
    public void backup(Path backupDir) {
        if (!backupInProgress.compareAndSet(false, true)) {
            throw new BitsyException(BitsyErrorCodes.BACKUP_IN_PROGRESS);
        } else {
            try {
                File backupDirFile = backupDir.toFile();

                if (!backupDirFile.isDirectory()) {
                    throw new BitsyException(BitsyErrorCodes.BAD_BACKUP_PATH, "Expecting " + backupDir + " to be a folder");
                }

                // Flush the transaction buffer
                flushTxLog();

                // Enqueue the backup task
                BackupJob backupJob = new BackupJob(backupDir);
                veReorgBuf.addAndExecuteWork(backupJob);
                
                // Wait for the response
                try {
                    backupJob.getCountDownLatch().await();
                } catch (InterruptedException e) {
                    BitsyException toThrow = new BitsyException(BitsyErrorCodes.BACKUP_INTERRUPTED, "Exception while waiting for a backup to be performed", e);
                    
                    log.error("Error while backing up the database", toThrow);
                    
                    throw toThrow;
                }
                
                BitsyException toThrow = backupJob.getException();
                if (toThrow != null) {
                    throw toThrow;
                }
            } finally {
                backupInProgress.set(false);
            }
        }
    }
    
    /** This class represents a "flush-now" action on the transaction log */ 
    public class FlushNowJob implements ITxBatchJob {

    }
    
    /** This class handles the flushing of the Memory to TxLog double buffer */
    public class TxUnitFlusher implements BufferFlusher<TxUnit> {
        @Override
        public void flushBuffer(BufferName bufName, final List<TxUnit> workList) throws BitsyException, InterruptedException {
            // Queue the batch of transactions into the transaction log 
            txLogToVEBuf.addAndExecuteWork(new TxBatch(workList));
        }
    }
    
    /** This class handles the queueing of the TxLog to VE files double buffer, which performs the actual work of the TxLogWriteFlusher */
    public class TxBatchQueuer implements BufferQueuer<ITxBatchJob> {
        @Override
        public void onQueue(BufferName bufName, ITxBatchJob batchJob) throws BitsyException {
            if (batchJob instanceof FlushNowJob) {
                // Nothing to do -- the flush will be automatically triggered by TxLogFlush
            } else if (!(batchJob instanceof TxBatch)) {
                log.error("Unsupported type of work in TxLogFlushPotential: {}", batchJob.getClass());
            } else {
                TxBatch trans = (TxBatch)batchJob;

                CommittableFileLog cfl = (bufName == BufferName.A) ? txA : txB;

                prepareForAppend(cfl);

                BitsyException bex = null;
                try {
                    int size = 0;
                    for (TxUnit work : trans.getTxUnitList()) {
                        size += work.writeToFile(cfl);
                    }

                    // Force the contents into the TA/B file 
                    cfl.commit();

                    // Set the size to calculate potential
                    trans.setSize(size);
                    
                    log.trace("Wrote {} bytes to {}", size, cfl.getPath());
                } catch (BitsyException e) {
                    bex = e;
                    throw e;
                } finally {
                    // Done with the write -- others can proceed
                    for (TxUnit work : trans.getTxUnitList()) {
                        if (bex != null) {
                            work.setException(bex);
                        }

                        // Whether/not the operation was successful, the work can not be redone
                        work.getCountDownLatch().countDown();
                    }
                }
            }
        }
    }
    
    /** This class handles the flushing of TxLog to VE double buffer */
    public class TxBatchFlusher implements BufferFlusher<ITxBatchJob> {
        @Override
        public void flushBuffer(BufferName bufName, List<ITxBatchJob> x) throws BitsyException {
            // Write the transaction log to the appropriate V/E files
            CommittableFileLog txLogToFlush = (bufName == BufferName.A) ? txA : txB; 
            veReorgBuf.addAndExecuteWork(new TxLog(txLogToFlush));
            
            synchronized (flushCompleteSignal) {
                // An explicit flush operation using flushTxLog() will wait for this signal
                lastFlushedBuffer = bufName;
                log.debug("Tx log in buffer {} has been flushed", lastFlushedBuffer);
                flushCompleteSignal.notifyAll();
            }
        }
    }
    
    /** This class handles the queueing of the TxLog to VE files double buffer, which performs the actual work of the TxLogWriteFlusher */
    public class TxLogQueuer implements BufferQueuer<IVeReorgJob> {
        @Override
        public void onQueue(BufferName bufName, IVeReorgJob job) throws BitsyException {
            CommittableFileLog cflV = (bufName == BufferName.A) ? vA : vB;
            CommittableFileLog cflE = (bufName == BufferName.A) ? eA : eB;
            
            if (job instanceof TxLog) {
                // A transaction log must be flushed to V/E text files
                TxLog txLog = (TxLog)job;
                CommittableFileLog inputLog = txLog.getCommittableFileLog();
                CommittableFileLog otherTxLog = (inputLog == txA) ? txB : txA; 

                prepareForAppend(cflV);
                prepareForAppend(cflE);

                inputLog.openForRead(); 

                // Move and compact the transaction log into the vertex and edge logs
                Long nextTxCounter = otherTxLog.getCounter();
                assert nextTxCounter != null;

                CompactAndCopyTask cp = new CompactAndCopyTask(new CommittableFileLog[] {inputLog}, cflV, cflE, memStore, nextTxCounter);
                cp.run();

                log.debug("Done writing to: {} of size {}", cflV.getPath(), cflV.size());
                log.debug("Done writing to: {} of size {}", cflE.getPath(), cflE.size());

                txLog.setReorgPotDiff(cp.getOutputLines());

                // Zap the txLog for the next flush
                log.debug("Zapping transaction log {}", inputLog);
                inputLog.openForOverwrite(logCounter++);
                
            } else if (job instanceof BackupJob) {
                // A backup of V/E text files must be performed
                BackupJob backupJob = (BackupJob)job;
                Path backupDir = backupJob.getBackupDir();

                try {
                    // 1. Create empty tx logs
                    Long txACounter = txA.getCounter();
                    CommittableFileLog cflOutA = new CommittableFileLog(backupDir.resolve(Paths.get(TX_A_TXT)), true);
                    cflOutA.openForOverwrite(txACounter);
                    cflOutA.close();
                    
                    Long txBCounter = txB.getCounter();
                    CommittableFileLog cflOutB = new CommittableFileLog(backupDir.resolve(Paths.get(TX_B_TXT)), true);
                    cflOutB.openForOverwrite(txBCounter);
                    cflOutB.close();
                    
                    // 2. Copy V?.txt to VA.txt
                    cflV.close();
                    Path sourceV = cflV.getPath();
                    Path targetV = backupDir.resolve(Paths.get(V_A_TXT));
                    
                    log.debug("Copying {} to {}", sourceV, targetV);
                    Files.copy(sourceV, targetV, StandardCopyOption.REPLACE_EXISTING);

                    // 3. Copy E?.txt to EA.txt
                    cflE.close();
                    Path sourceE = cflE.getPath();
                    Path targetE = backupDir.resolve(Paths.get(E_A_TXT));
                    
                    log.debug("Copying {} to {}", sourceE, targetE);
                    Files.copy(sourceE, targetE, StandardCopyOption.REPLACE_EXISTING);

                    // 4. Copy meta?.txt to metaA.txt --  -- all metadata file ops are synchronized on the mA object
                    synchronized (mA) {
                        // Index copy must be synchronized on this class
                        CommittableFileLog cflM = getEarlierBuffer(mA, mB);
                        cflM.close();
                        Path sourceM = cflM.getPath();
                        Path targetM = backupDir.resolve(Paths.get(META_A_TXT));

                        log.debug("Copying {} to {}", sourceM, targetM);
                        Files.copy(sourceM, targetM, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (Exception e) {
                    backupJob.setException(new BitsyException(BitsyErrorCodes.BACKUP_FAILED, "Encountered exception while backing up the database to " + backupDir, e));
                } finally {
                    backupJob.getCountDownLatch().countDown();
                }
                
                log.info("Completed backup to directory {}", backupDir);
            }
        }
    }
    
    /** This class handles the reorganization of the V and E A/B files */
    public class TxLogFlusher implements BufferFlusher<IVeReorgJob> {
        @Override
        public void flushBuffer(BufferName bufName, List<IVeReorgJob> x) throws BitsyException {
            CommittableFileLog sourceV = (bufName == BufferName.A) ? vA : vB;
            CommittableFileLog sourceE = (bufName == BufferName.A) ? eA : eB;

            CommittableFileLog targetV = (bufName == BufferName.B) ? vA : vB;
            CommittableFileLog targetE = (bufName == BufferName.B) ? eA : eB;

            log.debug("Re-organizing {} and {} into {} and {} respectively", sourceV.getPath(), sourceE.getPath(), targetV.getPath(), targetE.getPath());
            
            // Open the source files for reading
            sourceV.openForRead();
            sourceE.openForRead();
            
            // Clear the target files and set the proper counter in the header
            targetV.openForOverwrite(logCounter++);
            targetE.openForOverwrite(logCounter++);
            
            // Find the lesser of the two counters -- synchronization is not
            // needed because tx logs can't be flushed in the middle of a re-org
            Long nextTxCounter = getEarlierBuffer(txA, txB).getCounter();
            assert (nextTxCounter != null);
            
            // Move and compact the source V/E files into the target V/E files
            CompactAndCopyTask cp = new CompactAndCopyTask(new CommittableFileLog[] {sourceV, sourceE}, targetV, targetE, memStore, nextTxCounter);
            cp.run();

            log.debug("Done writing to: {}. Post-reorg size {}", targetV.getPath(), targetV.size());
            log.debug("Done writing to: {}. Post-reorg size {}", targetE.getPath(), targetE.size());

            // Zap the source files for the next flush, and close them
            sourceV.openForOverwrite(null);
            sourceE.openForOverwrite(null);
            sourceV.close();
            sourceE.close();

            veReorgPotential.setOrigLines(cp.getOutputLines());
        }
    } 
    
    @Override
    public Collection<VertexBean> getAllVertices() {
        return memStore.getAllVertices();
    }

    @Override
    public Collection<EdgeBean> getAllEdges() {
        return memStore.getAllEdges();
    }

    @Override
    public synchronized <T extends Element> void createKeyIndex(String key, Class<T> elementType) {
        memStore.createKeyIndex(key, elementType);
        
        // Rewrite the metadata file -- all metadata file ops are synchronized on the mA object
        synchronized (mA) {
            saveVersionAndIndexes();
        }
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementType) {
        memStore.dropKeyIndex(key, elementType);

        // Rewrite the metadata file -- all metadata file ops are synchronized on the mA object
        synchronized (mA) {
            saveVersionAndIndexes();
        }
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementType) {
        return memStore.getIndexedKeys(elementType);
    }

    @Override
    public Collection<VertexBean> lookupVertices(String key, Object value) {
        return memStore.lookupVertices(key, value);
    }

    @Override
    public Collection<EdgeBean> lookupEdges(String key, Object value) {
        return memStore.lookupEdges(key, value);
    }

    @Override
    public boolean allowFullGraphScans() {
        return memStore.allowFullGraphScans();
    }
}
