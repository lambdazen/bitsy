package com.lambdazen.bitsy;

import com.lambdazen.bitsy.store.LoadTask;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

public class RecoveryTest extends FileBasedTestCase {
    public RecoveryTest() {}

    public void testPartialTx() throws Exception {
        for (long minSizeForParallelLoader : new long[] {10, 1024 * 1024}) {
            // Try with both the parallel and serial loaders
            LoadTask.MIN_SIZE_FOR_PARALLEL_LOADER = minSizeForParallelLoader;

            URL url = this.getClass().getClassLoader().getResource("recovery");
            Path rootPath = Paths.get(url.toURI());
            System.out.println("Path = " + rootPath);

            String[] fileNames =
                    new String[] {"txA.txt", "txB.txt", "vA.txt", "vB.txt", "eA.txt", "eB.txt", "metaA.txt", "metaB.txt"
                    };

            Path targetDir = tempDir("recoverytest");
            String[] paths = {"stage1", "stage2", "stage3", "stage4", "stage5", "stage6", "stage7"};

            int[] vCounts = new int[] {50, 50, 100, 75, 75, 101, 101};
            int[] eCounts = new int[] {0, 0, 0, 0, 0, 99, 99};

            for (int i = 0; i < paths.length; i++) {
                System.out.println("Deleting " + targetDir);
                deleteDirectory(targetDir.toFile(), false);

                Path sourceDir = rootPath.resolve("./" + paths[i]).normalize();
                System.out.println("Copying from " + sourceDir);
                for (String fileName : fileNames) {
                    Files.copy(
                            sourceDir.resolve(fileName),
                            targetDir.resolve(fileName),
                            StandardCopyOption.REPLACE_EXISTING);
                }

                BitsyGraph graph = new BitsyGraph(targetDir);

                checkIterCount(graph.vertices(), vCounts[i]);
                checkIterCount(graph.edges(), eCounts[i]);
                graph.shutdown();
            }
        }
    }

    private void checkIterCount(Iterator<?> iter, int expectedCount) {
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }

        assertEquals(expectedCount, count);
    }
}
