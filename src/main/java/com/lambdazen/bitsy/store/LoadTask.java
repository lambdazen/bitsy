package com.lambdazen.bitsy.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.store.Record.RecordType;
import com.lambdazen.bitsy.util.CommittableFileLog;
import com.lambdazen.bitsy.util.DefaultCommitChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class loads a list of input files to an (empty) graph store, which is
 * typically an in-memory store
 */
public class LoadTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LoadTask.class);

    /** Files larger than this setting (default 1MB) will use parallel record reader. Can be changed by the application */
    public static long MIN_SIZE_FOR_PARALLEL_LOADER = 1024 * 1024;

    CommittableFileLog[] inputs;
    MemoryGraphStore store;
    long totalVE;
    int opsPerNonTxCommit;
    ObjectMapper mapper;
    ObjectReader vReader, eReader;
    boolean repairMode;

    public LoadTask(
            CommittableFileLog[] inputs,
            MemoryGraphStore store,
            int opsPerNonTxCommit,
            ObjectMapper mapper,
            boolean safeMode) {
        this.inputs = inputs;
        this.store = store;
        this.totalVE = 0;
        this.opsPerNonTxCommit = opsPerNonTxCommit;
        this.mapper = mapper;
        this.vReader = mapper.readerFor(VertexBeanJson.class);
        this.eReader = mapper.readerFor(EdgeBeanJson.class);
        this.repairMode = safeMode;
    }

    public long getTotalVE() {
        return totalVE;
    }

    public void run() {
        IStringCanonicalizer canonicalizer = new SingleThreadedStringCanonicalizer();

        // Find the minimum counter among transaction logs which marks the end of an incomplete V/E txt file
        long lastTxLogNumber = Long.MAX_VALUE;
        for (int i = 0; i < inputs.length; i++) {
            inputs[i].openForRead();
            if (inputs[i].isTxLog() && (lastTxLogNumber > inputs[i].getCounter())) {
                lastTxLogNumber = inputs[i].getCounter().longValue();
            }
        }

        // Loop through files and load them
        BitsyException toThrow = null;
        for (int i = 0; i < inputs.length; i++) {
            CommittableFileLog inputLog = inputs[i];

            String fileName = inputLog.getPath().toString();
            int lineNo = 1; // Start with 1 because the header is the counter

            boolean isTxLog = inputLog.isTxLog();

            RecordReader recordReader;
            boolean usingSerialLoader;
            if (repairMode || isTxLog || (inputLog.size() < MIN_SIZE_FOR_PARALLEL_LOADER)) {
                log.debug("Using RecordReader for {}", inputLog);
                recordReader = new RecordReader(inputLog, vReader, eReader);
                usingSerialLoader = true;
            } else {
                log.debug("Using ParallelRecordReader for {}", inputLog);
                recordReader = new ParallelRecordReader(inputLog, 10000, vReader, eReader);
                usingSerialLoader = false;
            }

            DefaultCommitChanges cc = new DefaultCommitChanges();
            try {
                Record rec;
                while ((rec = recordReader.next()) != null) {
                    // log.debug("Loading line: {}", rec.getJson());
                    lineNo++;

                    // Logic to commit
                    if (isTxLog) {
                        // Commit only on Tx records. This is to ensure that the last bad Tx (if one exists) doesn't get
                        // loaded
                        if (rec.getType() == RecordType.T) {
                            // Keep track of this line in case we run into an error later
                            if (usingSerialLoader) {
                                inputLog.mark();
                            }

                            // Commit the changes
                            this.totalVE += store.saveChanges(cc, canonicalizer);

                            cc.reset();

                            // Process the next line in the while loop
                            continue;
                        }
                    }

                    // Insert/update V and E records
                    switch (rec.getType()) {
                        case L:
                            // This only happens for non-transactional logs
                            if (isTxLog) {
                                throw new BitsyException(
                                        BitsyErrorCodes.INTERNAL_ERROR,
                                        "Found an unexpected log (L) record in file " + fileName + " at line "
                                                + lineNo);
                            }

                            // If the record matches the last Tx, it must be truncated now.
                            if (Long.parseLong(rec.getJson()) == lastTxLogNumber) {
                                // Mark the L record's end to truncate
                                if (usingSerialLoader) {
                                    inputLog.mark();
                                }

                                // Check to see if this is the last line -- otherwise, the file must be truncated here
                                if (recordReader.next() == null) {
                                    // All OK
                                    continue;
                                } else {
                                    // And throw an exception to trigger the truncate
                                    throw new BitsyException(
                                            BitsyErrorCodes.INCOMPLETE_TX_FLUSH,
                                            "File " + inputLog + " has an L record in line " + lineNo
                                                    + " with the last valid Tx log number " + lastTxLogNumber);
                                }
                            }
                            break;

                        case T:
                            throw new BitsyException(
                                    BitsyErrorCodes.INTERNAL_ERROR,
                                    "Found an unexpected transaction (T) record in file " + fileName + " at line "
                                            + lineNo);

                        case E:
                            BitsyEdge edge = rec.getEdge();
                            if (isTxLog) {
                                // Add to commit log
                                cc.changeEdge(edge);
                            } else {
                                // Directly save into the store
                                totalVE = store.saveEdge(totalVE, edge, canonicalizer);
                            }
                            break;

                        case V:
                            BitsyVertex vertex = rec.getVertex();
                            if (isTxLog) {
                                // Add to commit log
                                cc.changeVertex(vertex);
                            } else {
                                // Directly save into the store
                                totalVE = store.saveVertex(totalVE, vertex, canonicalizer);
                            }
                            break;

                        default:
                            throw new BitsyException(
                                    BitsyErrorCodes.INTERNAL_ERROR,
                                    "Unhandled record type " + rec.getType() + " in file " + fileName + " at line "
                                            + lineNo);
                    }
                }

                // For Tx logs, rollback at the end (incomplete Tx). Otherwise commit.
                if (isTxLog) {
                    // Throw away the commit changes
                    int vCount = cc.getVertexChanges().size();
                    int eCount = cc.getEdgeChanges().size();
                    if ((vCount > 0) || (eCount > 0)) {
                        assert isTxLog;

                        log.warn(
                                "Throwing away {} vertices and {} edges that were not successfully committed",
                                vCount,
                                eCount);

                        if (inputLog.getMarkPosition() > 0) {
                            inputLog.truncateAtMark();
                        }

                        log.info("Recovery of {} is complete", fileName);
                    }
                } else {
                    this.totalVE += store.saveChanges(cc, canonicalizer);
                    cc.reset();
                }
            } catch (Exception e) {
                if (usingSerialLoader && isTxLog) {
                    // Fix the TX Log
                    log.warn("Recovering from exception while loading from file " + fileName + " at line " + lineNo, e);

                    if (inputLog.getMarkPosition() == -1) {
                        inputLog.openForOverwrite(inputLog.getCounter());
                        log.warn("Zapped file {} to recover from error", fileName);
                    } else {
                        // Truncate will log the warning
                        inputLog.truncateAtMark();
                    }

                    log.info("Recovery of {} is complete", fileName);

                    // Since this is the last file, we can return
                    return;
                }

                if (usingSerialLoader
                        && (e instanceof BitsyException)
                        && (((BitsyException) e).getErrorCode() == BitsyErrorCodes.INCOMPLETE_TX_FLUSH)) {
                    // Fix the V/E log
                    assert !isTxLog : "Only loading V/E logs can throw INCOMPLETE_TX_FLUSH exception";

                    log.warn("Recovering from an incomplete flush operation from a transactional log", e);

                    // A Tx flush was not complete
                    inputLog.truncateAtMark();

                    log.info("Recovery of {} is complete", fileName);

                    // Continue to the next file in the for loop
                } else {
                    // Unrecoverable exception
                    toThrow = new BitsyException(
                            BitsyErrorCodes.DATABASE_IS_CORRUPT,
                            "The database files are corrupt. Please restore a backup version",
                            e);

                    if (repairMode) {
                        log.error("Encountered an unrecoverable exception while loading the database", toThrow);
                    } else {
                        log.info("Encountered exception while loading without the repair mode", e);
                        throw toThrow;
                    }
                }
            } finally {
                // Close is in finally to make sure that the input files are closed before next reorg
                try {
                    inputLog.close();
                } catch (BitsyException e) {
                    if (toThrow != null) {
                        toThrow = e;
                    }
                }
            }

            if (toThrow != null) {
                throw toThrow;
            }
        }
    }
}
