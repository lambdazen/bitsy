package com.lambdazen.bitsy.util;

import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.store.FileBackedMemoryGraphStore;
import com.lambdazen.bitsy.store.Record;
import com.lambdazen.bitsy.store.Record.RecordType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class provides wrapper methods to append, commit and reset a file. It is not thread safe. */
public class CommittableFileLog {
    private static final Logger log = LoggerFactory.getLogger(CommittableFileLog.class);

    /** Should the database lock files before writing to them? Yes by default, but can be changed by the application. **/
    public static boolean LOCK_MODE = true;

    // 32K buffer for reading files during re-organization
    private static final int BUFFER_SIZE = 1 * 1024 * 1024;

    // Common fields
    Path filePath;
    FileChannel fileChannel;
    boolean isTxLog;
    Long counter;

    // Fields capturing the read state
    byte[] byteArr = new byte[BUFFER_SIZE];
    ByteBuffer byteBuf = ByteBuffer.wrap(byteArr);
    CharBuffer charBuf;
    int byteIndex = 0;
    int index = 0;
    boolean endReached = false;
    StringBuilder curLine = new StringBuilder(1024); // 1K long initial size -- to avoid resizing
    long markPosition = -1;

    // Field capturing write state
    private boolean writeMode = false;

    // TODO: Better way to keep track of byteIndex
    private static final int mask2 = 0xFF80;
    private static final int mask3 = 0xF800;

    public CommittableFileLog(Path filePath, boolean isTxLog) throws IOException {
        this.filePath = filePath;
        this.isTxLog = isTxLog;
        this.counter = null;
    }

    public Long getCounter() {
        return counter;
    }

    public boolean isTxLog() {
        return isTxLog;
    }

    public Path getPath() {
        return filePath;
    }

    public void resetReadBuffers() {
        charBuf = null;
        index = 0;
        byteIndex = 0;
        endReached = false;
        curLine = new StringBuilder();
        markPosition = -1;
        writeMode = false;
    }

    // Re-implementing readLine() to allow truncate
    public String readLine() {
        while (!endReached) {
            if (charBuf == null) { //  || (charBuf.length() <= index)
                // Read next
                byteBuf.clear();
                int bytesRead;
                try {
                    bytesRead = fileChannel.read(byteBuf);
                } catch (IOException e) {
                    throw new BitsyException(
                            BitsyErrorCodes.ERROR_READING_FROM_FILE,
                            "File " + getPath() + " can not be opened for reading",
                            e);
                }

                if (bytesRead == -1) {
                    // Done with reads
                    endReached = true;

                    break; // or continue, doesn't matter
                }

                // Get more chars into the buffer
                byteBuf.flip();

                charBuf = FileBackedMemoryGraphStore.utf8.decode(byteBuf);

                // Reset the index
                byteIndex = 0;
                index = 0;
            }

            // Find where the \n is
            int cbLen = charBuf.length();
            int oldIndex = index;
            for (; index < cbLen; index++, byteIndex++) {
                char ch = charBuf.charAt(index);

                // Handling UTF-8 characters. byteIndex is used for truncation
                if (((int) ch & mask2) != 0) {
                    byteIndex++;
                }

                if (((int) ch & mask3) != 0) {
                    byteIndex++;
                }

                if (ch == '\n') {
                    break; // out of the for loop
                }
            }

            // Add what was found (or till end) to the current line
            CharBuffer partialLine = charBuf.subSequence(oldIndex, index);

            // Did we reach the end of the buffer?
            if (index < cbLen) {
                // Skip the \n next time
                index++;
                byteIndex++;

                if (curLine.length() == 0) {
                    // Common case
                    return partialLine.toString();
                } else {
                    curLine.append(partialLine);

                    // Exit the while-loop and return curLine
                    break;
                }
            } else {
                // Reached end of buffer. Continue reading to the charBuf
                charBuf = null;

                // But add what was found to curLine
                curLine.append(partialLine);
            }
        }

        // Return what is left in the string buffer
        if (endReached && curLine.length() == 0) {
            return null;
        } else {
            String ans = curLine.toString();
            curLine.setLength(0);
            return ans;
        }
    }

    public void mark() {
        mark(0);
    }

    public void mark(int numBytesBehind) {
        try {
            assert (fileChannel != null);

            long ans = fileChannel.position() - numBytesBehind;
            if (charBuf != null) {
                // A char buffer read ahead already
                // Adjust the position to point it to after the last readLine
                ans = ans - byteBuf.limit() + byteIndex;
                // log.debug("Index is " + byteIndex + ". Char index " + index + ". Position is " +
                // fileChannel.position() + ". Buffer size " + byteBuf.limit());
            }

            this.markPosition = ans;
        } catch (IOException e) {
            throw new BitsyException(
                    BitsyErrorCodes.ERROR_WRITING_TO_FILE, "File " + getPath() + " could not be marked", e);
        }
    }

    public long getMarkPosition() {
        return markPosition;
    }

    public void truncateAtMark() {
        // Truncate at mark can only be called when the fileChannel is active
        assert (fileChannel != null);
        assert (markPosition != -1);

        try {
            log.info(
                    "Truncating {} to recover from crash. {} bytes removed",
                    getPath(),
                    (fileChannel.size() - markPosition));

            fileChannel.truncate(markPosition);
        } catch (IOException e) {
            throw new BitsyException(
                    BitsyErrorCodes.ERROR_WRITING_TO_FILE,
                    "File " + getPath() + " could not truncated at marked position " + markPosition,
                    e);
        }
    }

    public void openForAppend() {
        if (writeMode) {
            return;
        }

        if (fileChannel != null) {
            close();
        }

        try {
            // Try opening the file to append, and create it if necessary
            fileChannel = FileChannel.open(filePath, StandardOpenOption.APPEND);

            // Lock it to avoid conflicts
            if (LOCK_MODE) {
                fileChannel.lock();
            }

            writeMode = true;
        } catch (IOException e) {
            throw new BitsyException(
                    BitsyErrorCodes.ERROR_WRITING_TO_FILE, "File " + getPath() + " can not be opened to append", e);
        }
    }

    public void openForOverwrite(Long counter) {
        // When opening for overwriting, it doesn't matter if the state is already WRITE.
        // It must be overwritten
        if (fileChannel != null) {
            close();
        }

        try {
            // Try opening the file to write, and zap if it exists
            fileChannel = FileChannel.open(
                    filePath,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);

            // Lock it to avoid conflicts
            if (LOCK_MODE) {
                fileChannel.lock();
            }

            // Write header if the counter is defined
            if (counter != null) {
                fileChannel.write(ByteBuffer.wrap(
                        Record.generateDBLine(RecordType.H, "" + counter).getBytes(FileBackedMemoryGraphStore.utf8)));
            }

            // Save the meta-data in case it was created
            fileChannel.force(true);

            // Update the counter value
            this.counter = counter;

            // Set the write mode to true
            writeMode = true;
        } catch (IOException e) {
            throw new BitsyException(
                    BitsyErrorCodes.ERROR_WRITING_TO_FILE, "File " + getPath() + " can not be opened to overwrite", e);
        }
    }

    public boolean exists() {
        return Files.exists(getPath());
    }

    public void openForRead() {
        // Set write mode to false -- safe to be false rather than true
        writeMode = false;

        // Open the file for read, independent of the state
        if (fileChannel != null) {
            close();
        }

        // Open using classic IO packages
        String header;
        try {
            // Try opening the file to read
            fileChannel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE);

            fileChannel.position(0);

            resetReadBuffers();

            header = readLine();
        } catch (IOException e) {
            throw new BitsyException(
                    BitsyErrorCodes.ERROR_READING_FROM_FILE, "File " + getPath() + " can not be opened for reading", e);
        }

        if (header == null) {
            // Empty file
            this.counter = null;
        } else {
            Record rec;
            try {
                rec = Record.parseRecord(header, 1, getPath().toString());
            } catch (BitsyException e) {
                // Error parsing the line
                throw new BitsyException(
                        BitsyErrorCodes.ERROR_IN_FILE_HEADER,
                        "File " + getPath() + " has a header line has an invalid checksum. Encountered: " + header,
                        e);
            }

            if (rec.getType() != RecordType.H) {
                throw new BitsyException(
                        BitsyErrorCodes.ERROR_IN_FILE_HEADER,
                        "File " + getPath() + " has a header line with an invalid record type. Encountered: " + header);
            }

            try {
                this.counter = new Long(rec.getJson());
            } catch (NumberFormatException e) {
                throw new BitsyException(
                        BitsyErrorCodes.ERROR_IN_FILE_HEADER,
                        "File " + getPath() + " has a non-numeric header counter. Encountered: " + header);
            }
        }
    }

    /** This method appends a line to the file channel */
    public void append(byte[] toWrite) throws BitsyException {
        ByteBuffer buf = ByteBuffer.wrap(toWrite);

        append(buf);
    }

    public void append(ByteBuffer buf) throws BitsyException {
        try {
            while (buf.hasRemaining()) {
                fileChannel.write(buf);
            }
        } catch (IOException e) {
            BitsyException be =
                    new BitsyException(BitsyErrorCodes.ERROR_WRITING_TO_FILE, "Could not write to " + toString(), e);
            log.error("Raised exception", be);
            throw be;
        }
    }

    public void commit() throws BitsyException {
        try {
            // TODO: Write-ahead on Txlogs to use force(false)
            fileChannel.force(true);
        } catch (IOException e) {
            BitsyException be =
                    new BitsyException(BitsyErrorCodes.ERROR_WRITING_TO_FILE, "Could not write to " + toString(), e);

            log.error("Raised exception", be);
            throw be;
        }
    }

    public void close() {
        writeMode = false;

        try {
            if (fileChannel != null) {
                fileChannel.force(true);
                fileChannel.close();
            }

            fileChannel = null;
        } catch (IOException e) {
            log.error("Ignored error encountered while closing file ${}", getPath(), e);

            // Ignore exceptions. All operations explicitly commit
        }
    }

    public String toString() {
        return "CommittableFileLog(" + filePath + ")";
    }

    public long size() {
        if (fileChannel == null) {
            return -1;
        } else {
            try {
                return fileChannel.size();
            } catch (IOException e) {
                return -1;
            }
        }
    }

    public static void setLockMode(boolean b) {
        LOCK_MODE = b;
    }
}
