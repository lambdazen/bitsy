package com.lambdazen.bitsy.structure;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR;

import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.BitsyIsolationLevel;
import com.lambdazen.bitsy.wrapper.BitsyAutoReloadingGraph;

public class BitsyTestGraphProvider extends AbstractGraphProvider {
	private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(BitsyAutoReloadingGraph.class);
//        add(BitsyGraph.class);
    }};

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

//		Thread.sleep(10);
//        File directory = new File(configuration.getString(BitsyGraph.DB_PATH_KEY));
//        wipeOut(directory);
//		Thread.sleep(10);
	}

	@Override
	public Set<Class> getImplementations() {
		return IMPLEMENTATION;
	}

	/*
    public Path tempDir(String dirName) throws IOException {
        return tempDir(dirName, true);
    }
    
    public Path tempDir(String dirName, boolean delete) throws IOException {

//    	String clsUri = this.getClass().getName().replace('.','/') + ".class";
//        URL url = this.getClass().getClassLoader().getResource(clsUri);
//        String clsPath = url.getPath();
//        System.out.println(clsPath);
//        String dir = clsPath.substring(clsPath.indexOf(":") + 1, clsPath.length() - clsUri.length());
//        System.out.println(dir);
//        Path root = Paths.get(dir).resolve("../" + dirName).normalize();

    	Path root = computeTestDataRoot().toPath();
    	System.out.println(root);
        
        if (!Files.exists(root)) {
        	System.out.println("Will create: " + root);
            Files.createDirectory(root);
        }
        
        if (!Files.exists(root)) {
        	throw new IOException("Couldn't create " + root);
        } else {
        	System.out.println("Created: " + root);
        }
        
        if (delete) {
            deleteDirectory(root.toFile(), false);
        }
        
        return root;
    }
*/

    private void wipeOut(File directory) {
        //System.out.println("Will wipe out: " + directory);
    	deleteDirectory(directory, false);
        //System.out.println("Done wiping out directory: " + directory);

        // overkill code, simply allowing us to detect when data dir is in use.  useful though because without it
        // tests may fail if a database is re-used in between tests somehow.  this directory really needs to be
        // cleared between tests runs and this exception will make it clear if it is not.
//        if (directory.exists()) {
//            throw new RuntimeException("unable to delete directory " + directory.getAbsolutePath());
//        }
    }

	protected static void deleteDirectory(final File directory, boolean deleteDir) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                	//System.out.println("Deleting dir: " + file.toString());
                    deleteDirectory(file, true);
                } else {
                	//System.out.println("Deleting file: " + file.toString());
                    file.delete();
                }
            }
            
            if (deleteDir) {
            	//System.out.println("Deleting dir: " + directory);
                directory.delete();
            }
        }
        //System.out.println("Exiting delete dir");
    }

/*
    public File computeTestDataRoot(String testMethodName) {
        final String clsUri = this.getClass().getName().replace('.', '/') + ".class";
        final URL url = this.getClass().getClassLoader().getResource(clsUri);
        final String clsPath = url.getPath();
        final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
        return new File(root.getParentFile(), "test-data-" + testMethodName);
    }
*/
	@Override
	public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName,
			GraphData loadGraphWith) {
		final String directory = makeTestDirectory(graphName, test, testMethodName);
		File testDataRootFile = new File(directory);
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

		return new HashMap<String, Object>() {{
            put(Graph.GRAPH, BitsyAutoReloadingGraph.class.getName());
            put(BitsyGraph.DB_PATH_KEY, testDataRoot);
            put(BitsyGraph.CREATE_DIR_IF_MISSING_KEY, true);
            put(BitsyGraph.DEFAULT_ISOLATION_LEVEL_KEY, BitsyIsolationLevel.READ_COMMITTED.toString());
            put(BitsyGraph.VERTEX_INDICES_KEY, "name, foo");
        }};
	}

}
