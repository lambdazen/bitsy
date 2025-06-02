package com.lambdazen.bitsy.util;

import com.lambdazen.bitsy.store.FileBackedMemoryGraphStore;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;

public class CommittableFileLogTest extends TestCase {
    public CommittableFileLogTest() {}

    public void testRead() throws Exception {
        File tempFile = File.createTempFile("mobydick", ".txt");

        InputStream is = getClass().getResourceAsStream("mobydick.txt");
        assertNotNull(is);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, FileBackedMemoryGraphStore.utf8));

        CommittableFileLog cflWrite = new CommittableFileLog(tempFile.toPath(), false);
        cflWrite.openForOverwrite(1L);

        Random rand = new Random();
        int expectedLines = -1;
        for (int run = 0; run < 10; run++) {
            List<String> lines = new ArrayList<String>();
            String line = null;
            while ((line = br.readLine()) != null) {
                lines.add(line);
                if (run == 0) {
                    cflWrite.append((line + '\n').getBytes(FileBackedMemoryGraphStore.utf8));
                }
            }

            br.close();
            is.close();

            if (run == 0) {
                cflWrite.close();
            }

            if (expectedLines != -1) {
                assertEquals(expectedLines, lines.size());
            }

            System.out.println("Finished writing to " + tempFile);

            List<String> nioLines = new ArrayList<String>();
            CommittableFileLog cfl = new CommittableFileLog(tempFile.toPath(), false);
            cfl.openForRead();

            line = null;
            int byteCounter = 13; // Start with header
            int linesDeleted = 0;
            while ((line = cfl.readLine()) != null) {
                // System.out.println("Line: " + line);
                nioLines.add(line);
                int lineBytes = 1 + line.getBytes(FileBackedMemoryGraphStore.utf8).length;
                byteCounter += lineBytes;

                if (rand.nextDouble() < 0.1) {
                    cfl.mark();
                    assertEquals(byteCounter, cfl.getMarkPosition());
                    linesDeleted = 0;
                } else if (rand.nextDouble() < 0.15) {
                    cfl.mark(lineBytes);
                    // System.out.println("Line bytes: " + lineBytes);
                    assertEquals(byteCounter - lineBytes, cfl.getMarkPosition());
                    linesDeleted = 1;
                } else {
                    linesDeleted++;
                }
            }

            assertEquals(lines.size(), nioLines.size());
            expectedLines = lines.size() - linesDeleted;

            for (int i = 0; i < lines.size(); i++) {
                assertEquals(lines.get(i), nioLines.get(i));
            }

            cfl.truncateAtMark();
            cfl.close();

            // For the next run -- use the temp file for reading
            is = Files.newInputStream(tempFile.toPath());
            br = new BufferedReader(new InputStreamReader(is, FileBackedMemoryGraphStore.utf8));

            // Skip the first line with the header
            br.readLine();
        }
    }
}
