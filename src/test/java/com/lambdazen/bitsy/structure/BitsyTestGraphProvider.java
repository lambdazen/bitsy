package com.lambdazen.bitsy.structure;

import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.BitsyIsolationLevel;
import com.lambdazen.bitsy.wrapper.BitsyAutoReloadingGraph;
import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR;

public class BitsyTestGraphProvider extends AbstractGraphProvider {
    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {
        {
            add(BitsyAutoReloadingGraph.class);
        }
    };

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null) {
            if (graph.tx().isOpen()) graph.tx().close();
            graph.tx().onReadWrite(READ_WRITE_BEHAVIOR.MANUAL);
            graph.tx().open();
            System.out.println("Clearing graph");
            graph.vertices().forEachRemaining(v -> v.remove());
            graph.tx().commit();
            System.out.println("Shutting down graph " + graph);
            graph.close();
        }
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    private void wipeOut(File directory) {
        deleteDirectory(directory, false);
    }

    protected static void deleteDirectory(final File directory, boolean deleteDir) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    // System.out.println("Deleting dir: " + file.toString());
                    deleteDirectory(file, true);
                } else {
                    // System.out.println("Deleting file: " + file.toString());
                    file.delete();
                }
            }

            if (deleteDir) {
                // System.out.println("Deleting dir: " + directory);
                directory.delete();
            }
        }
        // System.out.println("Exiting delete dir");
    }

    @Override
    public Map<String, Object> getBaseConfiguration(
            String graphName, Class<?> test, String testMethodName, GraphData loadGraphWith) {
        final String directory = makeTestDirectory(graphName, test, testMethodName);
        File testDataRootFile = Paths.get(directory).toFile();
        testDataRootFile.mkdirs();

        try {
            if (testDataRootFile.exists()) {
                wipeOut(testDataRootFile);
            }
        } catch (Exception ex) {
            System.out.println("SETUP FAILED!!!");
            ex.printStackTrace();
        }

        String testDataRoot = testDataRootFile.getPath();

        return new HashMap<String, Object>() {
            {
                put(Graph.GRAPH, BitsyAutoReloadingGraph.class.getName());
                put(BitsyGraph.DB_PATH_KEY, testDataRoot);
                put(BitsyGraph.CREATE_DIR_IF_MISSING_KEY, true);
                put(BitsyGraph.DEFAULT_ISOLATION_LEVEL_KEY, BitsyIsolationLevel.READ_COMMITTED.toString());
                put(BitsyGraph.VERTEX_INDICES_KEY, "name, foo");
            }
        };
    }
}
