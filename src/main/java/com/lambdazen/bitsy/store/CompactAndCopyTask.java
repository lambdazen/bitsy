package com.lambdazen.bitsy.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.IGraphStore;
import com.lambdazen.bitsy.store.Record.RecordType;
import com.lambdazen.bitsy.util.CommittableFileLog;

/**
 * This class takes an array of input file logs and appends their contents (in
 * order) it to separate V and E file logs. It automatically checks the store to
 * see if the records are latest, and drops any records that are obsolete.
 */
public class CompactAndCopyTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CompactAndCopyTask.class);
    
    // 4KB initial buffer size to avoid too many resizings 
    private static final int INIT_STRING_BUFFER_SIZE = 4096;
    
    CommittableFileLog[] inputs;
    CommittableFileLog vLog;
    CommittableFileLog eLog;
    IGraphStore store;
    int addedLines;
    long nextTxCounter;
    
    public CompactAndCopyTask(CommittableFileLog[] inputs, CommittableFileLog vos, CommittableFileLog eos, IGraphStore store, long nextTxCounter) {
        this.inputs = inputs;
        this.vLog = vos;
        this.eLog = eos;
        this.store = store;
        this.addedLines = 0;
        this.nextTxCounter = nextTxCounter;
    }
    
    public void run() {
        StringBuffer tempV = new StringBuffer(INIT_STRING_BUFFER_SIZE);
        StringBuffer tempE = new StringBuffer(INIT_STRING_BUFFER_SIZE);
        
        // If the first log is not a tx log, this is a reorg
        // A reorg goes through the entire set of database files, which means
        // that D records can be dropped.
        boolean isReorg = !inputs[0].isTxLog();
        
        int i = 0;
        String fileName = null;
        int lineNo = 0;
        
        BitsyException toThrow = null;
        try {
            for (i=0; i < inputs.length; i++) {
                CommittableFileLog inputLog = inputs[i];

                try {
                    fileName = inputLog.getPath().toString();

                    inputLog.openForRead();

                    // In transational mode, only transactions that are completed will be writted to V/E txt files
                    boolean isTransactional = inputLog.isTxLog() && (i == inputs.length - 1); // Only the last TX LOG may be incomplete in recovery mode

                    String line;
                    lineNo = 0;
                    while ((line = inputLog.readLine()) != null) {
                        lineNo++;

                        // When parsing a record
                        Record rec = Record.parseRecord(line, lineNo, fileName);

                        if (rec.checkObsolete(store, isReorg, lineNo, fileName)) {
                            //log.debug("Ignoring obsolete record {}", line);
                            continue;
                        }

                        RecordType recType = rec.getType();
                        if ((recType == RecordType.E) || (recType == RecordType.V)) {
                            addedLines++;
                        }

                        if (!isTransactional) {
                            if (lineNo % 10000 == 0) {
                                // Write out the temporary data to the files
                                vLog.append(tempV.toString().getBytes(FileBackedMemoryGraphStore.utf8));
                                eLog.append(tempE.toString().getBytes(FileBackedMemoryGraphStore.utf8));

                                // Reset the temporary buffers
                                tempV.setLength(0);
                                tempE.setLength(0);
                            }
                            
                            // No special handling is needed
                            switch (recType) {
                            case T: // Transaction marker can be ignored
                            case L: // Old log marker can be ignored -- only happens during rollover
                                break;

                            case E:
                                //eLog.append(line.getBytes(FileBackedMemoryGraphStore.utf8));
                                //eLog.append(newLine);
                                tempE.append(line);
                                tempE.append('\n');
                                break;

                            case V: 
                                //vLog.append(line.getBytes(FileBackedMemoryGraphStore.utf8));
                                //vLog.append(newLine);
                                tempV.append(line);
                                tempV.append('\n');
                                break;

                            default:
                                throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Unhandled record type " + recType + " in file " + fileName + " at line " + lineNo);
                            }
                        } else {
                            // A transactional file -- this requires each block that ends with a T records to be flushed 
                            switch (recType) {
                            case L: // Old log marker can be ignored
                                break;

                            case T:
                                // Write out the temporary data to the files
                                vLog.append(tempV.toString().getBytes(FileBackedMemoryGraphStore.utf8));
                                eLog.append(tempE.toString().getBytes(FileBackedMemoryGraphStore.utf8));

                                // Reset the temporary buffers
                                tempV.setLength(0);
                                tempE.setLength(0);

                                // All set
                                break;

                            case E:
                                tempE.append(line);
                                tempE.append('\n');
                                break;

                            case V: 
                                tempV.append(line);
                                tempV.append('\n');
                                break;

                            default:
                                throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Unhandled record type " + recType + " in file " + fileName + " at line " + lineNo);
                            }
                        }
                    }
                    
                    if (!isTransactional) {
                        // Write out the temporary data to the files
                        vLog.append(tempV.toString().getBytes(FileBackedMemoryGraphStore.utf8));
                        eLog.append(tempE.toString().getBytes(FileBackedMemoryGraphStore.utf8));

                        // Reset the temporary buffers
                        tempV.setLength(0);
                        tempE.setLength(0);
                    }
                } finally {
                    // Close is in finally to make sure that the input files are closed before next reorg
                    inputLog.close();
                }
            }
            
            // After all inputs log(s) have been processed, an L entry is added to recover the V/E logs in case of crash in the middle of the NEXT copy process
            String logRec = Record.generateDBLine(RecordType.L, "" + nextTxCounter);
            vLog.append(logRec.getBytes(FileBackedMemoryGraphStore.utf8));
            eLog.append(logRec.getBytes(FileBackedMemoryGraphStore.utf8));
        } catch (BitsyException e) {
            // There was an error in the hash-code or elsewhere. This is not a recoverable error -- may be the next load can fix it.
            log.error("Unrecoverable error while copying database files during a " + ((isReorg) ? "reorganization" : "transaction flush"), e);

            toThrow = e;
        } catch (Exception e) {
            // This is an IO error
            toThrow = new BitsyException(BitsyErrorCodes.ERROR_READING_FROM_FILE, "File " + fileName + " at line " + lineNo, e); 
            log.error("Unrecoverable error while copying database files during a " + ((isReorg) ? "reorganization" : "transaction flush"), e);
        } finally {
            try {
                vLog.commit();
            } catch (BitsyException e) {
                log.error("Error while committing file " + vLog.getPath(), e);
                if (toThrow != null) {
                    toThrow = e;
                }
            }

            try {
                eLog.commit();
            } catch (BitsyException e) {
                log.error("Error while committing file " + eLog.getPath(), e);
                if (toThrow != null) {
                    toThrow = e;
                }
            }
        }
            
        // The exception may have happened in the catch-block or inside the commit. But only the first one (toThrow) is the root cause  
        if (toThrow != null) {
            throw toThrow;
        }
    }
    
    public int getOutputLines() {
        return addedLines;
    }
}
