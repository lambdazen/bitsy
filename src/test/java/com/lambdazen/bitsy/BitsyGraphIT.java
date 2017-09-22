package com.lambdazen.bitsy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import com.lambdazen.bitsy.store.FileBackedMemoryGraphStore;
import com.lambdazen.bitsy.util.CommittableFileLog;

public class BitsyGraphIT extends FileBasedTestCase {
    Graph graph;
    Path dbPath;
    Random rand = new Random();
    private Throwable toThrow;
    
    public boolean isPersistent() {
        return true;
    }
    
    public void setUp() throws IOException {
        CommittableFileLog.setLockMode(false);
        
        setUp(true);
    }
    
    public void setUp(boolean delete) throws IOException {
        System.out.println("Setting up graph");
        
        this.dbPath = tempDir("temp-bitsy-graph", delete);
        graph = new BitsyGraph(dbPath);

//        this.dbPath = tempDir("temp-neo4j-graph", delete);
//        graph = new Neo4jGraph(dbPath.toString());
    }

    protected static void deleteDirectory(final File directory) {
        deleteDirectory(directory, true);
    }
    
    protected static void deleteDirectory(final File directory, boolean deleteDir) {
        if (directory.exists()) {
            for (File file : directory.listFiles()) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
            
            if (deleteDir) {
                directory.delete();
            }
        }
    }
    
    public void tearDown() {
        System.out.println("Tearing down graph");
        try {
        	graph.close();
        } catch (Exception e) {
        	throw new RuntimeException("Got ex", e);
        }        
    }

    private Vertex getVertex(Graph graph, Object id) {
    	Iterator<Vertex> iter = graph.vertices(id);
    	if (iter.hasNext()) {
    		return iter.next();
    	} else {
    		return null;
    	}
    }

    private Edge getEdge(Graph graph, Object id) {
    	Iterator<Edge> iter = graph.edges(id);
    	if (iter.hasNext()) {
    		return iter.next();
    	} else {
    		return null;
    	}
    }

    private Edge addEdge(Graph graph, Vertex vOut, Vertex vIn, String label) {
    	return vOut.addEdge(label, vIn);
    }

    private void removeEdge(Graph graph, Edge edge) {
    	edge.remove();
    }

    private void removeVertex(Graph graph, Vertex vertex) {
    	vertex.remove();
    }
    
    public void testLoop() throws IOException {
        int numRuns = 3;
        for (int run = 0; run < numRuns; run++) {
            //System.out.println("Run #" + run);

            int numPerCommit = 10;
            int numCommit = 10;
            int numVertices = numPerCommit * numCommit;
            Object[] vids = new Object[numVertices];

            long ts = System.currentTimeMillis();
            for (int i = 0; i < numVertices; i++) {
                Vertex v = graph.addVertex();
                v.property("rand", rand.nextInt());
                v.property("count", i);
                vids[i] = v.id();

                if (i % numPerCommit == 0) {
                    graph.tx().commit();
                }

                // Make sure the vertex is there in the middle of the Tx
                assertEquals(new Integer(i), getVertex(graph, vids[i]).value("count"));
            }
            graph.tx().commit();

            double duration = System.currentTimeMillis() - ts;
            System.out.println("Took " + duration + "ms to insert " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");

            // Make sure vertices are there
            for (int i = 0; i < numVertices; i++) {
                Vertex v = getVertex(graph, vids[i]);
                assertEquals(i, (int)v.value("count"));
            }

            // if (rand.nextDouble() < 2) {
            // return;
            // }

            // Stop and start
            if (isPersistent()) {
                tearDown();
                setUp(false);
            }

            // Make sure vertices are still there
            for (int i = 0; i < numVertices; i++) {
                Vertex v = getVertex(graph, vids[i]);
                assertNotNull(v);
                assertEquals(i, (int)v.value("count"));
            }

            // Now add edges
            Object[] eids = new Object[numVertices];
            for (int i = 0; i < numVertices; i++) {
                Vertex vOut = getVertex(graph, vids[i]);
                Vertex vIn = getVertex(graph, vids[(i + 1) % numVertices]);
                Edge e = addEdge(graph, vOut, vIn, "foo");
                e.property("rand", rand.nextInt());
                e.property("count", i);

                eids[i] = e.id();

                if (i % numPerCommit == 0) {
                    graph.tx().commit();
                }

                // Make sure the edge is there in the middle of the Tx
                // Using toString() to get the edge
                Edge qEdge = getEdge(graph, eids[i].toString());
                assertEquals(new Integer(i), (Integer)qEdge.value("count"));
                assertEquals(vOut, qEdge.outVertex());
                assertEquals(vIn, qEdge.inVertex());
            }
            graph.tx().commit();

            // Make sure that the edges are there
            for (int i = 0; i < numVertices; i++) {
                Edge e = getEdge(graph, eids[i]);
                assertEquals(i, (int)e.value("count"));
            }

            // Now modify the edges. Delete even ones
            for (int i = 0; i < numVertices; i++) {
                Edge e = getEdge(graph, eids[i]);
                e.property("count", i + 1);

                eids[i] = e.id();

                if (i % numPerCommit == 0) {
                    graph.tx().commit();
                    
                    try {
                        e.value("foo");
                    } catch (BitsyException ex) {
                        assertEquals(BitsyErrorCodes.ACCESS_OUTSIDE_TX_SCOPE, ex.getErrorCode());
                    }
                }

                // Make sure the edge is there in the middle of the Tx
                Edge qEdge = getEdge(graph, eids[i]);
                assertEquals(new Integer(i + 1), qEdge.value("count"));
                
                // Traverse to the vertices
                Vertex vOut = qEdge.outVertex();
                assertEquals(new Integer(i), vOut.value("count"));
                
                Vertex vIn = qEdge.inVertex();
                assertEquals(new Integer((i + 1) % numVertices), vIn.value("count"));
                
                // Run queries in the middle of the Tx
                assertEquals(qEdge, vIn.edges(Direction.IN).next());
                assertEquals(qEdge, vOut.edges(Direction.OUT).next());
                
                if (i % 2 == 0) {
                    removeEdge(graph, qEdge);

                    // Run queries in the middle of the Tx
                    if (rand.nextBoolean()) {
                        assertFalse(vIn.edges(Direction.IN).hasNext());
                        assertFalse(vOut.edges(Direction.OUT).hasNext());
                    } else {
                        try {
                            vIn.edges(Direction.IN).next();
                            fail("Expecting no element");
                        } catch (NoSuchElementException e1) {
                        }
                        
                        try {
                            vOut.edges(Direction.OUT).next();
                            fail("Expecting no element");
                        } catch (NoSuchElementException e2) {
                        }
                    }
                    
                    try { 
                        vIn.edges(Direction.IN).next();
                        fail("Can't access a missing edge");
                    } catch (NoSuchElementException ex) {
                        // Good
                    }
                    
                    try {
                        qEdge.value("count");
                        fail("Can not access deleted edges");
                    } catch (BitsyException ex) {
                        assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, ex.getErrorCode());
                    }
                }
            }
            graph.tx().commit();

            // Stop and start
            if (isPersistent()) {
                tearDown();
                //assertTrue(rand.nextDouble() > 5);
                setUp(false);
            }

            // Make sure vertices are still there
            for (int i = 0; i < numVertices; i++) {
                Vertex v = getVertex(graph, vids[i]);
                // System.out.println("Looking for " + vids[i]);
                
                assertEquals(i, (int)v.value("count"));
            }

            // Make sure that the edges are there
            for (int i = 0; i < numVertices; i++) {
                Edge e = getEdge(graph, eids[i]);
                
                if (i % 2 == 0) {
                    assertNull(e);
                } else {
                    // i was changed to i+1
                    assertNotNull("Could not find edge for UUID " + eids[i], e);
                    assertEquals(i + 1, (int)e.value("count"));
                }
            }
            
            // Stop and start
            if (isPersistent()) {
                tearDown();
                setUp(false);
            }
            
            // Make sure vertices are still there. Modify odd ones and delete even ones
            graph.tx().onReadWrite(READ_WRITE_BEHAVIOR.MANUAL);
            if (!graph.tx().isOpen()) graph.tx().open();

            for (int i = 0; i < numVertices; i++) {
                Vertex v = getVertex(graph, vids[i]);
                assertEquals(i, (int)v.value("count"));
                
                if (i % 2 == 0) {
                    removeVertex(graph, v);
                } else {
                    v.property("count", i + 1);
                }

                if (i % numPerCommit == 0) {
                    graph.tx().commit();

                    try {
                        v.value("count");
                    } catch (IllegalStateException ex) {
                    	// That's expected
                        // TP2 behavior: assertEquals(BitsyErrorCodes.ACCESS_OUTSIDE_TX_SCOPE, ex.getErrorCode());
                    }

                    graph.tx().open();
                }
                
                // Check the vertex in the middle of the Tx
                Vertex qv = getVertex(graph, vids[i]);
                if (i % 2 == 0) {
                    assertNull(qv);
                } else {
                    assertEquals(i + 1, (int)v.value("count"));
                }
            }
            graph.tx().commit();
            graph.tx().onReadWrite(READ_WRITE_BEHAVIOR.AUTO);
            
            // Make sure that only odd vertices are still there 
            for (int i = 0; i < numVertices; i++) {
                Vertex v = getVertex(graph, vids[i]);
                
                if (i % 2 == 0) {
                    assertNull(v);
                } else {
                    assertEquals(i + 1, (int)v.value("count"));
                }
            }
            
            // Make sure that the edges are gone (even vertices are deleted)
            for (int i = 0; i < numVertices; i++) {
                Edge e = getEdge(graph, eids[i]);
                assertNull(e);
            }
            
            // Stop and start
            if (isPersistent()) {
                tearDown();
                setUp(false);
            }
            
            // Make sure that the edges are gone (even vertices are deleted)
            for (int i = 0; i < numVertices; i++) {
                Edge e = getEdge(graph, eids[i]);
                assertNull(e);
            }
            
            // Make sure that only odd vertices are still there 
            for (int i = 0; i < numVertices; i++) {
                Vertex v = getVertex(graph, vids[i]);
                
                if (i % 2 == 0) {
                    assertNull(v);
                } else {
                    assertEquals(i + 1, (int)v.value("count"));
                }
            }
        }
    }
    
    public void testClique() throws IOException {
        // Tests indexes in a clique graph
        int numPerCommit = 10;
        int numVertices = 29;
        int numRuns = 3;
        
        BitsyGraph kig = (BitsyGraph)graph;
        if (!kig.getIndexedKeys(Vertex.class).contains("vmod3")) {
            kig.createKeyIndex("vmod3", Vertex.class);
        } else {
            kig.dropKeyIndex("vmod3", Vertex.class);
            kig.createKeyIndex("vmod3", Vertex.class);
        }
        
        if (!kig.getIndexedKeys(Edge.class).contains("emod3")) {
            kig.createKeyIndex("emod3", Edge.class);
        } else {
            kig.dropKeyIndex("emod3", Edge.class);
            kig.createKeyIndex("emod3", Edge.class);
        }
        
        int txc = 0;
        Object[] vids = new Object[numVertices];
        Object[][] eids = new Object[numVertices][numVertices];
        long startLong = rand.nextLong();
        for (int run = 0; run < numRuns; run++) {
            if ((run == numRuns - 1) && ((BitsyGraph)graph).getStore().allowFullGraphScans()) {
                // Last run without index
                kig.dropKeyIndex("vmod3", Vertex.class);
                kig.dropKeyIndex("emod3", Edge.class);
            }
            
            // Change the starting long to look for index replacement logic
            long oldStartLong = startLong;
            startLong = rand.nextLong();
            int[] edgeCount = new int[] {0, 0, 0}; 
            
            for (int i=0; i < numVertices; i++) {
                if (i == numVertices / 2) {
                    if (isPersistent()) {
                        tearDown();
                        
                        setUp(false);
                    }
                }

                Vertex v;
                if  (run == 0) {
                    v = graph.addVertex();
                    vids[i] = v.id();
                } else {
                    v = getVertex(graph, vids[i]);
                    assertEquals(vids[i], v.id());
                }

                v.property("vmod3", startLong + i % 3);
                
                // Make sure that the index works on committed and uncommitted data
                if ((txc++ % numPerCommit == 0) && rand.nextDouble() < 2) {
                    graph.tx().commit();
                    
                    // Add a random vertex without this property
                    Vertex dummyV = graph.addVertex();
                    if (i % 2 == 0) dummyV.property("notvmod3", 1);
                }
                
//                System.out.println("Testing vertex " + i + ". Run #" + run);
                
                // Negative test on index value in previous iter
                if (i > numVertices - 3) {
                    if (rand.nextBoolean()) {
                        assertFalse(((BitsyGraph)graph).verticesByIndex("vmod3", oldStartLong + i % 3).hasNext());
                    } else {
                        try {
                            ((BitsyGraph)graph).verticesByIndex("vmod3", oldStartLong + i % 3).next();
                            fail("Expecting no element");
                        } catch (NoSuchElementException e1) {
                        }
                    }
                }
                
                Iterator<BitsyVertex> viter = ((BitsyGraph)graph).verticesByIndex("vmod3", startLong +  i % 3);
                int count = 0;
                while (viter.hasNext()) {
                    Vertex qv = viter.next();

                    boolean matchesSomething = false;
                    for (int div3 = i % 3; div3 <= i; div3 += 3) {
                        if (vids[div3].equals(qv.id())) {
                            matchesSomething = true;
                            //System.out.println("Found match for " + div3);
                        }
                        matchesSomething = true;
                    }
                    assertTrue(matchesSomething);

                    count++;
                    //System.out.println("Count = " + count);
                }
                
                assertEquals(1 + i / 3, count);
                
                // Now add edges
                for (int j=0; j < i; j++) {
                    Edge e;
                    if (run == 0) {
                        Vertex outV = getVertex(graph, vids[j]);
                        Vertex inV =  getVertex(graph, vids[i]);

                        e = addEdge(graph, outV, inV, "test");
                        eids[i][j] = e.id();
                    } else {
                        e = getEdge(graph, eids[i][j]);
                    }
                    
                    e.property("emod3", "E" + (startLong + ((i-j) % 3)));
                    Object eid = e.id();

                    // Make sure that the index works on committed and uncommitted data
                    if (txc++ % numPerCommit == 0) {
                        graph.tx().commit();
                    }
                    
                    // Negative test on index value in previous iter
                    if ((i == numVertices - 1) && (j >= numVertices - 3)) {
                        assertFalse(((BitsyGraph)graph).edgesByIndex("emod3", "E" + (oldStartLong + ((i-j) % 3))).hasNext());
                    }

                    //System.out.println("Testing edge from " + j + " to " + i);
                    Iterator<BitsyEdge> eiter = ((BitsyGraph)graph).edgesByIndex("emod3", "E" + (startLong + ((i-j) % 3)));
                    edgeCount[(i-j) % 3]++;
                    
                    int ecount = 0;
                    boolean matchesThis = false;
                    while (eiter.hasNext()) {
                        Edge qe = eiter.next();

                        if (eid.equals(qe.id())) {
                            matchesThis = true;
                            //System.out.println("Found edge match");
                        }
                        
                        ecount++;
                    }
                    
                    assertTrue(matchesThis);
                    assertEquals(edgeCount[(i-j) % 3], ecount);
                }
                
                graph.tx().commit();
            }
        }
        
        // Remove vertex props
        for (int i=0; i < numVertices; i++) {
            Vertex v = getVertex(graph, vids[i]);
            v.property("vmod3").remove();
            
            Iterator<Edge> vOut = v.edges(Direction.OUT);
            while (vOut.hasNext()) {
                vOut.next().property("emod3").remove();
            }
            
            // Add a random vertex without this property
            Vertex dummyV = graph.addVertex();
            dummyV.property("notvmod3", 1);
            Object dummyVId = dummyV.id();
            
            // Commit & remove
            graph.tx().commit();
            dummyV = getVertex(graph, dummyVId);
            removeVertex(graph, dummyV);
        }

        // Make sure the index is empty
        for (int i=0; i < 3; i++) {
            assertFalse(((BitsyGraph)graph).verticesByIndex("vmod3", startLong + i % 3).hasNext());
        }

        // Add it back
        for (int i=0; i < numVertices; i++) {
            Vertex v = getVertex(graph, vids[i]);
            assertNull(v.value("vmod3"));
            v.property("vmod3", startLong);
        }

        // Make sure the index is non-empty before commit
        assertTrue(((BitsyGraph)graph).verticesByIndex("vmod3", startLong).hasNext());
        
        graph.tx().commit();

        // ... and after
        for (int i=0; i < 3; i++) {
            assertTrue(((BitsyGraph)graph).verticesByIndex("vmod3", startLong).hasNext());
        }
        
        // Remove it again
        for (int i=0; i < numVertices; i++) {
            Vertex v = getVertex(graph, vids[i]);
            v.property("vmod3").remove();
        }
        graph.tx().commit();
        
        for (int i=0; i < 3; i++) {
            assertFalse(((BitsyGraph)graph).verticesByIndex("vmod3", startLong + i % 3).hasNext());
        }
        graph.tx().commit();
        
        // Remove edge props
        for (int i=0; i < numVertices; i++) {
            for (int j=0; j < i; j++) {
                if (eids[i][j] == null) {
                    continue;
                }

                Edge e = getEdge(graph, eids[i][j]);
                Property prop = e.property("emod3");
                if (prop.isPresent()) prop.remove();
            }
            
            // Add a random edge without this property
            Vertex dummyV1 = graph.addVertex();
            dummyV1.property("notvmod3", 1);
            
            Vertex dummyV2 = graph.addVertex();
            dummyV2.property("notvmod3", 1);
            
            Edge e = addEdge(graph, dummyV1, dummyV2, "test");
            e.property("notemod3", "E123");
        }

        for (int i=0; i < numVertices; i++) {
            for (int j=0; j < i; j++) {
                if (eids[i][j] == null) {
                    continue;
                }
                assertFalse(((BitsyGraph)graph).edgesByIndex("emod3", "E" + (startLong + ((i-j) % 3))).hasNext());
            }
        }
        
        graph.tx().commit();
        
        for (int i=0; i < numVertices; i++) {
            for (int j=0; j < i; j++) {
                if (eids[i][j] == null) {
                    continue;
                }
                assertFalse(((BitsyGraph)graph).edgesByIndex("emod3", "E" + (startLong + ((i-j) % 3))).hasNext());
            }
        }

        // Remove edges
        for (int i=0; i < numVertices; i++) {
            for (int j=0; j < i; j++) {
                if ((i + j) % 10 == 0) {
                    getEdge(graph, eids[i][j]).remove();
                }
            }
        }
        graph.tx().commit();
        
        // Remove vertices
        for (int i=0; i < numVertices; i++) {
            getVertex(graph, vids[i]).remove();
        }

        graph.tx().commit();
    }
    
    public void testTypes() throws IOException {
        int numPerCommit = 10;
        int numCommit = 10;
        int numVertices = numPerCommit * numCommit;
        Object[] vids = new Object[numVertices];

        long ts = System.currentTimeMillis();
        for (int i = 0; i < numVertices; i++) {
            Vertex v = graph.addVertex();
            v.property("int", i);
            v.property("long", (long) i);
            v.property("Integer", new Integer(i));
            v.property("Long", new Long(i));
            v.property("double", (double) i + 0.1);
            v.property("Double", new Double(i + 0.1));
            v.property("float", (float) i + 0.1);
            v.property("Float", new Float(i + 0.1));
            v.property("string", "String");
            v.property("date", new Date(ts));

            v.property("stringArr", new String[] { "foo", "bar" });
            v.property("intArr", new int[] { 1, 2 });
            v.property("longArr", new long[] { 1, 2 });
            v.property("doubleArr", new double[] { 1.1, 2.1 });
            v.property("floatArr", new float[] { 1.1f, 2.1f });
            v.property("booleanArr", new boolean[] { false, true });

            vids[i] = v.id();

            if (i % numPerCommit == 0) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();

        double duration = System.currentTimeMillis() - ts;
        System.out.println("Took " + duration + "ms to insert " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");

        // Stop and start
        if (isPersistent()) {
            tearDown();
            setUp(false);
        }
        
        // Make sure vertices are still there
        for (int i = 0; i < numVertices; i++) {
            Vertex v = getVertex(graph, vids[i]);

            assertNotNull("Could not find " + vids[i], v);
            //System.out.println("Index " + v.getId() + ". All props: " + v.getPropertyKeys());
            assertEquals(i, (int)v.value("int"));
            assertEquals((long) i, (long)v.value("long"));
            assertEquals(new Integer(i), (Integer)v.value("Integer"));
            assertEquals(new Long(i), (Long)v.value("Long"));
            assertEquals((double) i + 0.1, (double)v.value("double"));
            assertEquals(new Double(i + 0.1), (Double)v.value("Double"));
//            assertEquals((float) i + 0.1, (float)v.value("float"));  TP3 doesn't support raw types property
            assertEquals(new Float(i + 0.1), (Float)v.value("Float"));
            assertEquals("String", v.value("string"));

            assertEquals("foo", ((String[]) v.value("stringArr"))[0]);
            assertEquals("bar", ((String[]) v.value("stringArr"))[1]);

            assertEquals(1, ((int[]) v.value("intArr"))[0]);
            assertEquals(2, ((int[]) v.value("intArr"))[1]);

            assertEquals(1, ((long[]) v.value("longArr"))[0]);
            assertEquals(2, ((long[]) v.value("longArr"))[1]);

            assertEquals(1.1d, ((double[]) v.value("doubleArr"))[0]);
            assertEquals(2.1d, ((double[]) v.value("doubleArr"))[1]);

            assertEquals(1.1f, ((float[]) v.value("floatArr"))[0]);
            assertEquals(2.1f, ((float[]) v.value("floatArr"))[1]);

            assertEquals(false, ((boolean[]) v.value("booleanArr"))[0]);
            assertEquals(true, ((boolean[]) v.value("booleanArr"))[1]);

            assertEquals(new Date(ts), v.value("date"));
        }
    }

    // TODO: Uncomment after supporting threaded transaction
/*
    public void testConcurrency() {
        // Create a vertex
        Vertex v = graph.addVertex();
        v.property("foo", "bar");
        
        Edge e = addEdge(graph, v, v, "self");
        e.property("foo", "bar");
        Object eid = e.id();
        
        Object vid = v.id();
        graph.tx().commit();

        // Create a threaded transaction
        //TransactionalGraph graph2 = ((ThreadedTransactionalGraph)graph).newTransaction(); // was startTransaction prior to 2.3.0 port
        Graph graph2 = graph.tx().createThreadedTx(); // TP 3.0 version
        
        Vertex v1 = getVertex(graph, vid);
        
        Edge e2 = getEdge(graph2, eid);
        Vertex v2 = e2.outVertex();
        
        assertEquals("bar", v1.value("foo"));
        v1.property("foo", "baz");
        
        assertEquals("bar", v2.value("foo"));
        v2.property("foo", "bart");

        // Should succeed
        graph.tx().commit();
        
        try {
            // Should fail
            graph2.tx().commit();
            
            fail("Failed optimistic concurrency test");
        } catch (BitsyRetryException ex) {
            assertEquals(BitsyErrorCodes.CONCURRENT_MODIFICATION, ex.getErrorCode());
        }

        Vertex v2retry = graph2.vertices(vid).next();
        assertEquals("baz", v2retry.value("foo"));
        
        v2retry.property("foo", "bart");
        graph2.tx().commit();
        
        try {
        	graph2.close();
        } catch (Exception ex) {
        	ex.printStackTrace();
        	fail("Couldn't close graph2" + ex);
        }
        
        // Ensure that the retried transaction came through
        Vertex v3 = getVertex(graph, vid);
        assertEquals("bart", v3.value("foo"));
        
        Edge e3 = v3.edges(Direction.OUT).next();
        assertEquals("self", e3.label());
        assertEquals("bar", e3.value("foo"));

        e3.property("foo", "baz");
        assertEquals("self", e3.label());
        assertEquals("baz", e3.value("foo"));

        //TransactionalGraph graph3 = ((ThreadedTransactionalGraph)graph).newTransaction(); // WAS startTransaction prior to 2.3.0 port
        Graph graph3 = graph.tx().createThreadedTx(); // TP3 version
        Edge e4 = getEdge(graph3, eid);
        
        assertEquals("self", e4.label());
        assertEquals("bar", e4.value("foo"));

        e4.property("foo", "bart");        
        assertEquals("self", e4.label());
        assertEquals("bart", e4.value("foo"));
        
        assertEquals(e4 + " doesnt' have foo: bart", "bart", e4.value("foo"));
        
        // Should pass
        graph3.tx().commit();

        // See if the changes from graph3 are seen here
        Vertex v51 = graph3.vertices(vid).next();
        Edge e51 = v51.edges(Direction.OUT).next();

        assertEquals("self", e51.label());
        assertEquals(e51 + " doesnt' have foo: bart", "bart", e51.value("foo"));
        
        try {
            graph.tx().commit();
            fail("Failed optimistic concurrency test");
        } catch (BitsyRetryException ex) {
            assertEquals(BitsyErrorCodes.CONCURRENT_MODIFICATION, ex.getErrorCode());
        }
        
        // See if the changes from graph3 are seen here
        Vertex v5 = getVertex(graph, vid);
        Edge e5 = v5.edges(Direction.OUT).next();
        
        assertEquals("self", e5.label());
        assertEquals(e5 + " doesnt' have foo: bart", "bart", e5.value("foo"));

        // All old vertices should be dead
        for (Element eN : new Element[] {v1, v2, v2retry, v3, e, e2, e3, e4}) {
            try {
                eN.value("foo");
                fail("dead Tx");
            } catch (BitsyException ex) {
                assertEquals(BitsyErrorCodes.ACCESS_OUTSIDE_TX_SCOPE, ex.getErrorCode());
            }
        }

        try {
        	graph.close();
        } catch (Exception ex) {
        	fail("Couldn't close graph " + ex);
        }
    }
*/
    // Pun intended 
    public void testEdgeCases() {
    	assertEquals(BitsyIsolationLevel.READ_COMMITTED, ((BitsyGraph)graph).getDefaultIsolationLevel());
    	for (BitsyIsolationLevel level : new BitsyIsolationLevel[] {BitsyIsolationLevel.READ_COMMITTED, BitsyIsolationLevel.REPEATABLE_READ}) {
    		((BitsyGraph)graph).setDefaultIsolationLevel(level);
    		graph.tx().commit();

    		int numVertices = 4;

    		Object[] vids = new Object[numVertices];
    		Vertex[] verts = new Vertex[numVertices];

    		for (int i=0; i < numVertices; i++ ) {
    			Vertex v = graph.addVertex();
    			vids[i] = v.id();
    			verts[i] = v;
    		}

    		// Add an edge from 0 to 1
    		Edge e01 = addEdge(graph, verts[0], verts[1], "one");
    		Object e01Id = e01.id();

    		Edge e01Alt = addEdge(graph, verts[0], verts[1], "two");
    		Object e01AltId = e01Alt.id();

    		Edge e12 = addEdge(graph, verts[1], verts[2], "empty"); // Empty string no longer allowed in TP3
    		Object e12Id = e12.id();

    		Edge e23 = addEdge(graph, verts[2], verts[3], "three");
    		Object e23Id = e23.id();

    		// Check if the edges are in
    		checkIterCount(verts[0].edges(Direction.OUT), 2);
    		checkIterCount(verts[0].edges(Direction.OUT, "one"), 1);
    		checkIterCount(verts[0].edges(Direction.OUT, "two"), 1);

    		checkIterCount(verts[1].edges(Direction.OUT), 1);
    		checkIterCount(verts[1].edges(Direction.IN), 2);

    		checkIterCount(verts[2].edges(Direction.OUT), 1);
    		checkIterCount(verts[2].edges(Direction.IN), 1);

    		checkIterCount(verts[3].edges(Direction.IN, "three"), 1);
    		checkIterCount(verts[3].edges(Direction.IN, "threeX"), 0);

    		// Transaction returns the same object on every get
    		for (int i=0; i < numVertices; i++) {
    			assertSame(verts[i], getVertex(graph, vids[i]));
    		}

    		assertSame(e01, getEdge(graph, e01.id()));
    		assertSame(e01Alt, getEdge(graph, e01Alt.id()));
    		assertSame(e12, getEdge(graph, e12.id()));
    		assertSame(e23, getEdge(graph, e23.id()));

    		// Now commit
    		graph.tx().commit();
    		
    		// Check to see if the vertices returned for the IDs are the same
    		for (int i=0; i < numVertices; i++) {
    			if (level == BitsyIsolationLevel.REPEATABLE_READ) {
    				Vertex v = getVertex(graph, vids[i]);
    				assertNotNull(v);
    				assertSame(v, getVertex(graph, vids[i]));
    			} else {
    				Vertex v = getVertex(graph, vids[i]);
    				assertNotNull(v);
    				assertNotSame(getVertex(graph, vids[i]), getVertex(graph, vids[i]));
    				assertEquals(getVertex(graph, vids[i]), getVertex(graph, vids[i]));
    				assertEquals(getVertex(graph, vids[i]).hashCode(), getVertex(graph, vids[i]).hashCode());
    			}
    		}

    		// Check to see if mid-tx deletes work
    		for (int i=0; i < numVertices; i++ ) {
    			verts[i] = getVertex(graph, vids[i]);
    		}

    		// Check to see if the end points of edges are the same objects as the loaded vertices
    		e23 = getEdge(graph, e23.id());
    		if (level == BitsyIsolationLevel.REPEATABLE_READ) {
    			assertSame(verts[2], e23.outVertex());
    			assertSame(verts[3], e23.inVertex());
    		} else {
    			assertEquals(verts[2].id(), e23.outVertex().id());
    			assertEquals(verts[3].id(), e23.inVertex().id());
    		}
    		
    		// Remove vertex 3
    		removeVertex(graph, verts[2]);

    		// The vertex should not be accessible
    		try {
    			verts[2].value("foo");
    			fail("Can't access deleted vertex");
    		} catch (BitsyException e) {
    			assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, e.getErrorCode());
    		}

    		// The edge should not be accessible
    		try {
    			e23.value("foo");
    			fail("Can't access deleted edge");
    		} catch (BitsyException e) {
    			assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, e.getErrorCode());
    		}

    		// Check to see that e12 and e23 disappeared from the queries as well
    		checkIterCount(verts[3].edges(Direction.IN), 0);
    		checkIterCount(verts[1].edges(Direction.OUT), 0);

    		// Now try to load an edge that was not previously in the transaction
    		e12 = getEdge(graph, e12Id);

    		// ... but that edge won't be visible because of the deleted vertex
    		assertNull(e12);

    		// Try the same for e23
    		e23 = getEdge(graph, e23.id());
    		assertNull(e23);

    		// No commit and recheck
    		graph.tx().commit();

    		// Make sure old elements are deleted
    		assertNull(getVertex(graph, vids[2]));
    		assertNull(getEdge(graph, e23Id));
    		assertNull(getEdge(graph, e12Id));

    		// Get an edge first
    		e01 = getEdge(graph, e01Id);

    		// ... then vertices
    		for (int i=0; i < numVertices; i++ ) {
    			verts[i] = getVertex(graph, vids[i]);
    		}

    		// ... and make sure the endpoints are the same
    		if (level == BitsyIsolationLevel.REPEATABLE_READ) {
    			assertSame(e01.inVertex(), verts[1]);
    			assertSame(e01.outVertex(), verts[0]);
    		} else {
    			assertEquals(e01.inVertex().id(), verts[1].id());
    			assertEquals(e01.outVertex().id(), verts[0].id());
    		}
    		
    		// Now remove the edge
    		removeEdge(graph, e01);

    		checkIterCount(verts[0].edges(Direction.OUT), 1); // only e01Alt lives
    		checkIterCount(verts[1].edges(Direction.IN), 1); // only e01Alt lives

    		// The edge should not be accessible
    		try {
    			e01.inVertex();
    			System.out.println("Fail!");
    			fail("Can't access deleted edge");
    		} catch (BitsyException e) {
    			assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, e.getErrorCode());
    		}

    		// But the vertex should be
    		e01Alt = getEdge(graph, e01AltId);
    		if (level == BitsyIsolationLevel.REPEATABLE_READ) {
    			assertSame(e01Alt.inVertex(), verts[1]);
        		assertSame(e01Alt.outVertex(), verts[0]);
    		} else {
    			assertEquals(e01Alt.inVertex().id(), verts[1].id());
    			assertEquals(e01Alt.outVertex().id(), verts[0].id());
    		}
    	}
    	
    	// Reset default isolation level
    	((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.REPEATABLE_READ);
    	graph.tx().commit();
    }

    public void testMultiThreadedEdgeQueries() throws IOException {
        // The purpose of this test is to create two vertices with a lot of
        // different types of edges with properties/labels. The edges will be
        // created and removed by multiple threads. A separate read thread will
        // validate that transaction boundaries are respected.
        setException(null);

        Vertex v1 = graph.addVertex();
        Vertex v2 = graph.addVertex();
        final Object v1Id = v1.id();
        final Object v2Id = v2.id();
        graph.tx().commit();

        final int numThreads = 100;
        ExecutorService service = Executors.newFixedThreadPool(numThreads + 2);
        long timeToTest = 10000; // 10 seconds
        final long timeToStop = System.currentTimeMillis() + timeToTest; 

        service.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    int counter = 0;
                    while (System.currentTimeMillis() < timeToStop) {
                        //System.out.println("Read iter");
                        counter++;
                        Vertex v1 = getVertex(graph, v1Id);
                        assertNotNull(v1);
    
                        Vertex v2 = getVertex(graph, v2Id);
                        assertNotNull(v2);
    
                        validate(v1.edges(Direction.OUT), v1, v2);
                        validate(v2.edges(Direction.IN), v1, v2);
    
                        // This is to reload new vertices in REPEATABLE_READ mode (default)
                        graph.tx().rollback();                    
                        
                        v1 = getVertex(graph, v1Id);
                        assertNotNull(v1);
    
                        v2 = getVertex(graph, v2Id);
                        assertNotNull(v2);
    
                        validate(v1.edges(Direction.OUT, "label34", "label17"), v1, v2, "label34", "label17");
                        validate(v2.edges(Direction.IN, "label87", "label39"), v1, v2, "label87", "label39");
    
                        // This is to reload new vertices in REPEATABLE_READ mode (default)
                        graph.tx().rollback();

                        //System.out.println("At " + counter + " read iterations");
                    }

                    System.out.println("Completed " + counter + " read iterations");
                } catch (Throwable e) {
                    setException(e);
                }
            }
            
            public void validate(Iterator<Edge> edges, Vertex v1, Vertex v2, String... labels) {
                Map<Integer, Integer> countMap = new HashMap<Integer, Integer>();
                List<String> labelsToCheck = ((labels == null) || (labels.length == 0)) ? null : Arrays.asList(labels);
                while (edges.hasNext()) {
                	Edge e = edges.next();
                	assertEquals(v1, e.outVertex());
                    assertEquals(v2, e.inVertex());
                    
                    Integer key = e.value("count");
                    Integer value = countMap.get(key);
                    if (value == null) {
                        countMap.put(key, 1);
                    } else {
                        countMap.put(key, value + 1);
                    }
                    
                    if (labelsToCheck != null) {
                        assertTrue("Could not find " + e.label() + " in " + labelsToCheck, labelsToCheck.contains(e.label()));
                    }
                }
                
                for (Map.Entry<Integer, Integer> entry : countMap.entrySet()) {
                    //System.out.println("Checking count for " + entry.getKey());
                    assertEquals("Transaction boundary not respected for " + entry.getKey() + ", got " + entry.getValue() + " edges", 
                            0, entry.getValue() % entry.getKey());
                }
            }
        });

        final int numEdges = 1;
        for (int i=0; i < numThreads; i++) {
            final String label = "label" + i;
            final int count = i;

            service.submit(new Runnable() {
                @Override
                public void run() {
                    int writeIters = 0;
                    try {
                        Object[] eids = new Object[count * numEdges];
                        int counter = 0;
                        
                        while (System.currentTimeMillis() < timeToStop) {
                            //System.out.println("Write iter");
                            Vertex v1 = getVertex(graph, v1Id);
                            assertNotNull(v1);
    
                            Vertex v2 = getVertex(graph, v2Id);
                            assertNotNull(v2);
    
                            for (int j=0; j < count; j++) {
                                Edge e = v1.addEdge(label, v2);
                                e.property("count", count);
                                eids[counter++] = e.id();
                            }
    
                            graph.tx().commit();
                            writeIters++;

                            try {
                                Thread.sleep(1 * numThreads);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            if (counter == eids.length) {
                                counter = 0;

                                // Hold at peak -- helps keep the set big
                                try {
                                    Thread.sleep(5 * numThreads);
                                } catch (InterruptedException e1) {
                                    e1.printStackTrace();
                                }
                                
                                // Clear and restart
                                int pauseCounter = 0;
                                for (Object eid : eids) {
                                    getEdge(graph, eid).remove();

                                    if ((pauseCounter > 0) && (pauseCounter++ % count == 0)) {
                                        writeIters++;
                                        graph.tx().commit();

                                        try {
                                            Thread.sleep(5);
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                        
                        System.out.println("Completed " + writeIters + " write iterations");
                    } catch (Throwable e) {
                        setException(e);
                    }
                }
            });    
        }
        
        try {
            Thread.sleep(timeToTest + 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (getException() != null) {
            throw new RuntimeException("Got exception", getException());
        }

        System.out.println("Done with multi-threaded edge tests");
        service.shutdown();
    }

    private void setException(Throwable t) {
        this.toThrow = t;
    }
    
    private Throwable getException() {
        return toThrow;
    }

    public void testMultiThreadedTreeCreation() throws IOException {
        // The purpose of this test multithreaded reads and writes. The graph is
        // a sub-graph of a tree whose root node has degree M. Each level
        // reduces the degree to M-1, ..., till 0. There are N threads that
        // start at the root vertex and traverse a random path to the leaf. At
        // each level, the thread checks that the degree is the correct value or
        // 0. If the children have not been created, the thread will create it
        // with some probability.
        //
        // One 'destructor' thread goes around the same tree and zaps nodes with
        // a probability of 1/m! where the degree is supposed to be m. All
        // threads may face CMEs.
        setException(null);

        // Added for TP3 because the default is now READ_COMMITTED
        ((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.REPEATABLE_READ);

        Vertex rootV = graph.addVertex();
        final Object rootVid = rootV.id(); 
        graph.tx().commit();

        final int rootDegree = 5; // 5! = 120 nodes -- keeping it low to increase chances of BitsyRetryException 
        final int numThreads = 10;
        ExecutorService service = Executors.newFixedThreadPool(numThreads + 1);

        long timeToTest = 20000; // 20 seconds
        final String labelId = "child";
        final long timeToStop = System.currentTimeMillis() + timeToTest; 

        for (int i=0; i < numThreads; i++) {
            service.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int createIters = 0;
                        while (System.currentTimeMillis() < timeToStop) {
                            Object vid = rootVid;
                            
                            int expectedDegree = rootDegree;
                            while (expectedDegree > 0) {
                                Thread.sleep(rand.nextInt(5)); // Sleep between 0 and 5ms;

                                Vertex v = null;
                                try {
                                    v = getVertex(graph, vid);
                                    int count = 0;
                                    int idx = rand.nextInt(expectedDegree);
                                    Iterator<Edge> edgeIter = v.edges(Direction.OUT, labelId);
                                    while (edgeIter.hasNext()) {
                                    	Edge e = edgeIter.next();
                                        if (count == idx) {
                                            vid = e.inVertex().id();
                                        }
                                        count++;
                                    }
                                    
                                    if (count > 0) {
                                        assertEquals(expectedDegree, count);
                                        expectedDegree--;
                                        continue;
                                    }
                                } catch (BitsyRetryException e) {
                                    // Someone else removed the edges
                                    System.out.println("Got a concurrent mod exception -- expected behavior");
                                    graph.tx().rollback();
                                    
                                    break;
                                }
                                                                
                                // This is the end because count is 0
                                if (expectedDegree == 0) {
                                    // Leaf is OK. Creator is happy. 
                                    assertEquals("no", v.value("haschildren"));
                                    break;
                                } else {                                    
                                    // Probability of 1/3 to create children
                                    if (rand.nextInt() % 3 == 0) {
                                        try {
                                            //System.out.println("Adding nodes with expected degree " + expectedDegree);

                                            // Not at leaf
                                            v.property("haschildren", "yes");
                                            for (int i=0; i < expectedDegree; i++) {
                                                Vertex child = graph.addVertex();
                                                child.property("haschildren", "no");
                                                v.addEdge(labelId, child);
                                            }
                                            
                                            createIters++;
                                            graph.tx().commit();
                                        } catch (BitsyRetryException e) {
                                            // Someone else did it
                                            System.out.println("Got a concurrent mod exception -- expected behavior");
                                            graph.tx().rollback();
                                        }
                                    }
                                    
                                    break;
                                }
                            }
                        }
                        
                        System.out.println("Completed " + createIters + " create iterations");
                    } catch (Throwable e) {
                        setException(e);
                    }
                }
            });
        }
        
        // Create the destructor
        service.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    int deleteIters = 0;
                    
                    while (System.currentTimeMillis() < timeToStop) {
                        Object vid = rootVid;
                        
                        int expectedDegree = rootDegree;
                        while (expectedDegree > 0) {
                            Thread.sleep(rand.nextInt(5)); // Sleep between 0 and 5ms;

                            Vertex v = getVertex(graph, vid);
                            int count = 0;
                            int idx = rand.nextInt(expectedDegree);
                            Iterator<Edge> edgeIter = v.edges(Direction.OUT, labelId);
                            while (edgeIter.hasNext()) {
                            	Edge e = edgeIter.next();
                                if (count == idx) {
                                    vid = e.inVertex().id();
                                }
                                count++;
                            }

                            if (count == 0) {
                                // Reached the end
                                break;
                            }

                            // There are children under this node -- toss a coin
                            if (rand.nextInt(factorial(expectedDegree)) == 0) {
                                try {
                                    //System.out.println("Removing node with expected degree " + expectedDegree);

                                    // Remove this node and its children
                                    removeDesc(v);

                                    deleteIters++;
                                    graph.tx().commit();
                                } catch (BitsyRetryException e) {
                                    // Someone else did it
                                    System.out.println("Got a concurrent mod exception -- expected behavior");
                                    graph.tx().rollback();
                                }
                                
                                break;
                            }

                            assertEquals(expectedDegree, count);
                            expectedDegree--;
                        }
                    }
                    
                    System.out.println("Completed " + deleteIters + " delete iters");
                } catch (Throwable e) {
                    setException(e);
                }
            }

            private void removeDesc(Vertex v) {
            	Iterator<Vertex> vertIter = v.vertices(Direction.OUT);
                while (vertIter.hasNext()) {
                	Vertex childV = vertIter.next();
                    removeDesc(childV);
                    childV.remove();
                }
            }

            private int factorial(int count) {
                int ans = 1;
                for (int i=1; i <= count; i++) {
                    ans *= count;
                }

                return ans;
            }
        });
        
        try {
            Thread.sleep(timeToTest + 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (getException() != null) {
            throw new RuntimeException("Got exception", getException());
        }

        System.out.println("Done with multi-threaded tree tests");
        service.shutdown();
    }

    private void checkIterCount(Iterator iter, int expectedCount) {
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        
        assertEquals(expectedCount, count);
    }
    
    public void testPersistence() throws Exception {
        BitsyGraph bGraph = (BitsyGraph)graph;
        FileBackedMemoryGraphStore store = (FileBackedMemoryGraphStore)(bGraph.getStore());
        
        // Check defaults
        assertEquals(1000, bGraph.getMinLinesPerReorg());
        assertEquals(4 * 1024 * 1024, bGraph.getTxLogThreshold());
        assertEquals(1d, bGraph.getReorgFactor());
        assertTrue(store.allowFullGraphScans());
        
        // Add a little -- shouldn't flush
        int numVertices = 100;
        Object[] vids = new Object[numVertices];
        Vertex[] verts = new Vertex[numVertices];
        
        for (int i=0; i < numVertices; i++ ) {
            Vertex v = graph.addVertex();
            v.property("foo", "something \n multi-line");
            vids[i] = v.id();
            verts[i] = v;
        }
        
        // Commit
        graph.tx().commit();
        
        // Make sure the number of lines are correct -- 100 + 1 header + 1 tx, 1 header in the other
        int txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        int txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        int vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        int vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        int eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        int eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));
        
        assertPair(102, 1, txA, txB); // 1 header + 100 V + 1 tx, 1 header 
        assertPair(0, 2, vA, eB); // An empty log is flushed in the beginning, hence 1, 2
        assertPair(0, 2, eA, eB);
        
        // Add some edges -- shouldn't flush
        for (int i=0; i < numVertices; i++ ) {
            verts[i] = getVertex(graph, vids[i]);
        }
        
        for (int i=1; i < numVertices; i++ ) {
            addEdge(graph, verts[i-1], verts[i], "label\n with multi-lines");
        }

        // Commit
        graph.tx().commit();
        
        // Make sure the number of lines are correct -- 100 + 1 header + 1 tx, 1 header in the other
        txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));
        
        assertPair(202, 1, txA, txB); // 1 header + 100 V + 1 tx + 99 E + 1tx, 1 header 
        assertPair(0, 2, vA, vB); // An empty log is flushed in the beginning, hence 2
        assertPair(0, 2, eA, eB);
        
        // Trigger a flush
        bGraph.setTxLogThreshold(10); // 10 bytes -- definitely will trigger flush on next add
        
        // Add dummy vertex with no props #101
        graph.addVertex();

        graph.tx().commit();
        
        // Wait for flush
        Thread.sleep(1000);
        
        // Make sure the number of lines are correct
        txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));
        
        assertPair(1, 1, txA, txB); // 1 header, 1 header 
        assertPair(0, 104, vA, vB); // 1 header + 1 log (on load) + 101 V + 1 log (on copy)
        assertPair(0, 102, eA, eB);   // 1 header + 1 log (on load) + 99 E + 1 log (on copy)

        Path stage1 = tempDir("stage1");
        bGraph.backup(stage1);
                
        // Set the min lines to a small value to trigger reorg -- more than 200 lines have been added, so the factor will kick in
        bGraph.setMinLinesPerReorg(1);
        assertEquals(bGraph.getMinLinesPerReorg(), 1);
        
        // Add a dummy vertex #102
        graph.addVertex();

        graph.tx().commit();
        
        // Wait for the flush followed by reorg
        Thread.sleep(2000);
        
        // Make sure the number of lines are correct
        txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));
        
        assertPair(1, 1, txA, txB); // 1 header, 1 header 
        assertPair(0, 104, vA, vB); // 1 header + 102 V + 1 L
        assertPair(0, 101, eA, eB);   // 1 header + 99 E + 1 L
        
        Path stage2 = tempDir("stage2");
        bGraph.backup(stage2);
        
        // Modify the vertices to see if that takes effect
        for (int i=0; i < numVertices; i++ ) {
            // Using the string representation
            verts[i] = getVertex(graph, vids[i].toString());
            verts[i].property("baz", new Date());
        }
        graph.tx().commit();
        
        // Lower reorg factor
        bGraph.setReorgFactor(0.0001d);
        bGraph.setMinLinesPerReorg(0);

        // Wait for the flush followed by reorg
        Thread.sleep(1000);
        
        // Add a dummy vertex #103
        graph.addVertex();
        graph.tx().commit();
        
        // Wait for the flush followed by reorg
        Thread.sleep(2000);
        
        // Increase the min lines and the factor to still trigger the reorg
        bGraph.setMinLinesPerReorg(99);
        bGraph.setReorgFactor(0.3d);
        bGraph.setTxLogThreshold(1024 * 1024); // 1MB -- won't trigger flush

        // Make sure the number of lines are correct
        txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));

        assertPair(1, 1, txA, txB); // 1 header, 1 header 
        assertPair(0, 105, vA, vB); // 1 header + 103 V + 1 L
        assertPair(0, 101, eA, eB);   // 1 header + 99 E + 1 L
        
        Path stage3 = tempDir("stage3");
        bGraph.backup(stage3);
        
        // Remove the vertices
        for (int i=0; i < numVertices; i++ ) {
            verts[i] = getVertex(graph, vids[i].toString());
            
            // This will remove both the vertex and edge
            removeVertex(graph, verts[i]);
        }
        graph.tx().commit();
        
        // Wait for the flush followed by reorg
        Thread.sleep(1000);
        
        // Make sure the number of lines are correct
        txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));

        assertPair(1, 102, txA, txB); // 1 header, 1 H + 100V header + 1 T 
        //assertPair(0, 105, vA, vB); // 1 header + 103 V + 1 L
        assertPair(0, 106, vA, vB); // 1 header + 103 V + 1 L
        //assertPair(0, 101, eA, eB);   // 1 header + 99 E + 1 L
        assertPair(0, 102, eA, eB);   // 1 header + 99 E + 1 L

        // Backup will flush the buffers explicitly. Reorg will follow after backup
        Path stage4 = tempDir("stage4");
        bGraph.backup(stage4);
        
        // Wait for the flush followed by reorg
        Thread.sleep(2000);
        
        // Make sure the number of lines are correct
        txA = lineCount(dbPath.resolve(Paths.get("txA.txt")));
        txB = lineCount(dbPath.resolve(Paths.get("txB.txt")));
        vA = lineCount(dbPath.resolve(Paths.get("vA.txt")));
        vB = lineCount(dbPath.resolve(Paths.get("vB.txt")));
        eA = lineCount(dbPath.resolve(Paths.get("eA.txt")));
        eB = lineCount(dbPath.resolve(Paths.get("eB.txt")));
        
        assertPair(1, 1, txA, txB); // 1 header, 1 header 
        assertPair(0, 5, vA, vB); // 1 header + 3 dummy V + 1 L
        assertPair(0, 2, eA, eB);   // 1 header + 0 E + 1 L
    }

    private void assertPair(int a, int b, int c, int d) {
        if (a == c) {
            assertEquals(b, d);
        } else {
            assertEquals(a, d);
            assertEquals(b, c);
        }
    }
    
    private int lineCount(Path file) throws Exception {
        InputStream is = new FileInputStream(file.toFile());
        assertNotNull(is);
        BufferedReader br = new BufferedReader(new InputStreamReader(is, FileBackedMemoryGraphStore.utf8));
        
        int ans = 0;
        String line = null;
        while ((line = br.readLine()) != null) {
            ans++;
        }
        
        br.close();
        is.close();
        
        return ans;
    }
    
    public void testLargeFileSaveLoad() throws Exception {
        BitsyGraph bGraph = (BitsyGraph)graph;
        
        if (!(bGraph.getStore() instanceof FileBackedMemoryGraphStore)) {
            return;
        }
        
        FileBackedMemoryGraphStore store = (FileBackedMemoryGraphStore)(bGraph.getStore());
        
        // Check defaults
        assertEquals(1000, bGraph.getMinLinesPerReorg());
        assertEquals(4 * 1024 * 1024, bGraph.getTxLogThreshold());
        assertEquals(1d, bGraph.getReorgFactor());
        assertTrue(store.allowFullGraphScans());
        
        // Add a little -- shouldn't flush
        int numCommit = 1000; // 5000
        int numPerCommit = 100; // 1000
        int numVertices = numCommit * numPerCommit;
        
        //Object[] vids = new Object[numVertices];
        //Vertex[] verts = new Vertex[numVertices];

        long ts = System.currentTimeMillis();
        Vertex prevV = null;
        for (int i=0; i < numVertices; i++ ) {
            Vertex v = graph.addVertex();
            v.property("foo", "bar");
            v.property("count", i);
            //vids[i] = v.getId();
            //verts[i] = v;
            
            if (prevV != null) {
                addEdge(graph, prevV, v, "test");
            }
            
            if (i % numPerCommit == 0) {
                prevV = null;
                //System.out.println("Commit");
                graph.tx().commit();                
            } else {
                prevV = v;
            }
        }
        
        // Commit
        graph.tx().commit();

        double duration = System.currentTimeMillis() - ts;
        System.out.println("Took " + duration + "ms to insert " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");

        bGraph.flushTxLog();
        
        duration = System.currentTimeMillis() - ts;
        System.out.println("Took " + duration + "ms to insert and flush " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");
        
        // Restart a few times
        for (int j=0; j<3; j++) {
            ts = System.currentTimeMillis();
            tearDown();
            
            duration = System.currentTimeMillis() - ts;
            System.out.println("Took " + duration + "ms to shut down graph with " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");

            ts = System.currentTimeMillis();

            setUp(false);

            duration = System.currentTimeMillis() - ts;
            System.out.println("Took " + duration + "ms to load " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");
            
            ts = System.currentTimeMillis();

            checkIterCount(graph.vertices(), numVertices);
            duration = System.currentTimeMillis() - ts;
            System.out.println("Took " + duration + "ms to query " + numVertices + " vertices. Rate = " + (duration / numVertices) + "ms per vertex");
        }        
    }

    public void XtestMultiThreadedCommits() throws Exception {
        for (int numThreads : new int[] {1, 2, 3, 4, 5, 10, 25, 50, 100, 150, 250, 500, 750, 1000}) {
            final int numVerticesPerThread = (numThreads <= 10 ? 10000 : (numThreads <= 100 ? 100000 : 100000)) / numThreads;
            int numElements = 2 * numVerticesPerThread * numThreads;
            
            ExecutorService service = Executors.newFixedThreadPool(numThreads);

            final Object[] startVertex = new Object[numThreads];

            final CountDownLatch cdl = new CountDownLatch(numThreads);
            long ts = System.currentTimeMillis();
            final String TEST_LABEL = "test";
            for (int i = 0; i < numThreads; i++) {
                final int tid = i;
                System.out.println("Scheduling write work for thread " + tid);
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        Object prevId = null;
                        for (int j = 0; j < numVerticesPerThread; j++) {
                            Vertex v = graph.addVertex();
                            if (prevId == null) {
                                startVertex[tid] = v.id();
                            } else {
                                Vertex prevV = getVertex(graph, prevId);
                                addEdge(graph, prevV, v, TEST_LABEL);
                            }
                            graph.tx().commit();
                            
                            prevId = v.id();
                        }
                        
                        System.out.println("Thread " + tid  + " is done");
                        cdl.countDown();
                    }
                });
            }
            
            cdl.await();

            double duration = System.currentTimeMillis() - ts;
            System.out.println("Took " + duration + "ms to save " + numElements + " vertices+edges. Rate = " + (duration / numElements) + "ms per vertex. TPS = " + ((double)numElements * 1000 / duration));
            
            // Wait 10 seconds between tests
            Thread.sleep(10000);
            
            ((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.READ_COMMITTED);
            final CountDownLatch cdl2 = new CountDownLatch(numThreads);
            ts = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                final int tid = i;
                System.out.println("Scheduling read work for thread " + tid);
                service.submit(new Runnable() {
                    @Override
                    public void run() {
                        for (int k=0; k < 100; k++) {
                            int count = 0;
                            Vertex v = getVertex(graph, startVertex[tid]);

                            Edge e;
                            do {
                                Iterator<Edge> eIter = v.edges(Direction.OUT);
                                if (!eIter.hasNext()) {
                                    break;
                                } else {
                                    count++;
                                    v = eIter.next().inVertex();
                                }
                            } while (true);

                            if (numVerticesPerThread != count + 1) {
                                System.out.println("Mistmatch between " + numVerticesPerThread + " and " + count);
                            }
                            
                            graph.tx().commit();
                        }
                        
                        System.out.println("Thread " + tid  + " is done");
                        cdl2.countDown();
                    }
                });
            }

            cdl2.await();

            duration = System.currentTimeMillis() - ts;
            System.out.println("Took " + duration + "ms to query " + numElements + " vertices+edge 100 times. Rate = " + (duration / numElements) + "ms per vertex. TPS = " + ((double)numElements * 100000 / duration));
            ((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.REPEATABLE_READ);

            service.shutdown();

            // Uncomment to look at memory usage
//            Thread.sleep(1000);
//            System.gc();
//            Thread.sleep(30000);

            // Clear graph
            tearDown();
            setUp(true);
        }
    }

    public void XtestMultiThreadedReadsOnBipartiteGraph() throws Exception {
        final int numVertices = 1000000; // 100K vertices
        final int numIters = 100000;
        final int numElements = 8 * numIters; // expected to visit 8 v/e per iteration
        final int partSize = numVertices / 2;
        final int numPerCommit = 1000;

        final String label = "test";
        final Object[] outVertices = new Object[partSize];
        final Object[] inVertices = new Object[partSize];
        
        // Vertices
        for (int i=0; i < partSize; i++) {
            outVertices[i] = graph.addVertex().id();
            inVertices[i] = graph.addVertex().id();
            
            if (i % numPerCommit == 0) {
                graph.tx().commit();
            }
        }
        
        // Edges
        for (int i=0; i < partSize; i++) {
            Vertex outVertex = getVertex(graph, outVertices[i]);
            outVertex.addEdge(label, getVertex(graph, inVertices[(5 * i + 1) % partSize]));
            outVertex.addEdge(label, getVertex(graph, inVertices[(5 * i + 4) % partSize]));
            outVertex.addEdge(label, getVertex(graph, inVertices[(5 * i + 7) % partSize]));

            if (i % numPerCommit == 0) {
                graph.tx().commit();
            }
        }
        graph.tx().commit();

        final int numRuns = 3;
        Map<Integer, String> calcStrMap = new HashMap<Integer, String>();
        
        for (int run=0; run < numRuns; run++) {
            ((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.READ_COMMITTED);
            for (final int numThreads : new int[] {1, 2, 3, 4, 5, 10, 25, 50, 100, 150, 250, 500, 750, 1000}) {
                ExecutorService service = Executors.newFixedThreadPool(numThreads);
    
                final CountDownLatch cdl = new CountDownLatch(numThreads);
                long ts = System.currentTimeMillis();
    
                ((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.READ_COMMITTED);
                System.out.println("Running bi-partite read test with " + numThreads + " threads");
                for (int i = 0; i < numThreads; i++) {
                    final int tid = i;
                    //System.out.println("Scheduling read work for thread " + tid);
                    service.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                Vertex v = getVertex(graph, outVertices[0]);
                                for (int k=0; k < 100 * numIters / numThreads; k++) {
                                    assertNotNull(v);
    
                                    Vertex nextV = randomVertex(v.vertices(Direction.OUT, label));
                                    
                                    assertNotNull(nextV);
        
                                    // Take a random edge back
                                    Vertex backV = randomVertex(nextV.vertices(Direction.IN));                                    
                                    if (backV != null) {
                                        v = backV;
                                    }
                                }
                                
                                //System.out.println("Thread " + tid  + " is done");
                            } catch (Throwable t) {
                                setException(t);
                            } finally {
                                cdl.countDown();
                            }
                        }

                        private Vertex randomVertex(Iterator<Vertex> vertices) {
                            List<Vertex> options = new ArrayList<Vertex>();
                            while (vertices.hasNext()) {
                            	Vertex option = vertices.next();
                                options.add(option);
                            }
                            
                            if (options.isEmpty()) {
                                return null;
                            } else {
                                return options.get(rand.nextInt(options.size()));
                            }
                        }
                    });
                }
    
                cdl.await();
                
                if (getException() != null) {
                    throw new RuntimeException("Error in testMultiThreadedReadsOnBipartiteGraph", getException());
                }
                
                service.shutdown();
    
                long duration = System.currentTimeMillis() - ts;
                double tps = ((double)numElements * 100000 / duration);
                System.out.println("Took " + duration + "ms to query " + numElements + " vertices+edge 100 times. Rate = " + (duration / numElements) + "ms per vertex. TPS = " + tps);

                String calcStr = calcStrMap.get(numThreads);
                if (calcStr == null) {
                    calcStrMap.put(numThreads, "=(" + tps);
                } else {
                    calcStrMap.put(numThreads, calcStr + " + " + tps);
                }
            }
        }

        for (Map.Entry<Integer, String> entry : calcStrMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue() + ")/3");
        }
        
        ((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.REPEATABLE_READ);
    }

    // TODO: Uncomment after supporting threaded transactions
/*
    public void testConcurrentAccessForIndexes() {
        // Create an index
        ((BitsyGraph)graph).dropKeyIndex("testKey", Vertex.class);
        ((BitsyGraph)graph).createKeyIndex("testKey", Vertex.class);
        
    	for (BitsyIsolationLevel level : new BitsyIsolationLevel[] {BitsyIsolationLevel.REPEATABLE_READ, BitsyIsolationLevel.READ_COMMITTED}) {
    		((BitsyGraph)graph).setDefaultIsolationLevel(level);
    		graph.tx().commit();
    		
	        Vertex v = graph.addVertex();
	        v.property("testKey", "foo");
	        
	        graph.tx().commit();
	        
	        Vertex v1 = graph.addVertex();
	        v1.property("testKey", "foo");
	        
	        // Don't commit yet
	        //TransactionalGraph anotherGraph = ((ThreadedTransactionalGraph)graph).newTransaction();
	        Graph anotherGraph = graph.tx().createThreadedTx(); // TP3 version
	        Iterator<BitsyVertex> indexResult = ((BitsyGraph)anotherGraph).verticesByIndex("testKey", "foo");
	        
	        checkIterCount(indexResult, 1); // Queried before the commit
	        
	        // Now commit the new vertex
	        graph.tx().commit();

	        indexResult = ((BitsyGraph)anotherGraph).verticesByIndex("testKey", "foo");
	        checkIterCount(indexResult, 2); // Queried after the commit -- the iterator refreshes itself
	        anotherGraph.tx().commit();
	        
	        //anotherGraph = ((ThreadedTransactionalGraph)graph).newTransaction();
	        anotherGraph = graph.tx().createThreadedTx();
	        indexResult = ((BitsyGraph)anotherGraph).verticesByIndex("testKey", "foo");
	        checkIterCount(indexResult, 2); // Queried after the commit
	        anotherGraph.tx().commit();
	        
	        getVertex(graph, v.id()).remove();
	        getVertex(graph, v1.id()).remove();
	        graph.tx().commit();
    	}
        
    	((BitsyGraph)graph).setDefaultIsolationLevel(BitsyIsolationLevel.REPEATABLE_READ);
        // Drop the index
        ((BitsyGraph)graph).dropKeyIndex("testKey", Vertex.class);
    }
*/
    
    // TODO: Uncomment after supporting threaded transaction
/*
    public void testConcurrentAccessForIndexes2() {
        // Create an index
        ((BitsyGraph)graph).dropKeyIndex("testKey", Vertex.class);
        ((BitsyGraph)graph).createKeyIndex("testKey", Vertex.class);
        
    	for (BitsyIsolationLevel level : new BitsyIsolationLevel[] {BitsyIsolationLevel.READ_COMMITTED, BitsyIsolationLevel.REPEATABLE_READ}) {
    	    System.out.println("Testing for " + level);
    		Vertex v = graph.addVertex();
	        v.property("testKey", "foo");
	        
	        graph.tx().commit();
	        
	        // Now read the vertex
	        ((BitsyGraph)graph).setTxIsolationLevel(level);
    		Vertex vBak = getVertex(graph, v.id());
	
	        // ... and update it in a different thread
	        //TransactionalGraph anotherGraph = graph.newTransaction();
    		Graph anotherGraph = graph.tx().createThreadedTx();
	        Vertex v2 = ((BitsyGraph)anotherGraph).verticesByIndex("testKey", "foo").next();
	        v2.property("testKey", "bar");
	        anotherGraph.tx().commit();
	        Vertex vBak2 = getVertex(anotherGraph, v2.id());
	        
	        // Re-query the old version with the new value
	        if (level == BitsyIsolationLevel.READ_COMMITTED) {
	        	Vertex vQuery = ((BitsyGraph)anotherGraph).verticesByIndex("testKey", "bar").next();
		        vQuery.property("testKey", "baz");
	        	// Now commit the new vertex -- should succeed
	        	graph.tx().commit();
	        } else {
	        	// TODO: Identify why this fails with TP3...
	        	//assertFalse(((BitsyGraph)anotherGraph).verticesByIndex("testKey", "bar").hasNext());
	        }
    	}
    	
        // Drop the index
        ((BitsyGraph)graph).dropKeyIndex("testKey", Vertex.class);
    }
*/
    
    public void testConcurrentAccessForVersions() throws InterruptedException {
        Vertex v = graph.addVertex();
        final Object vid = v.id();        
        v.property("testKey", "foo");
        graph.tx().commit();
        
        v = getVertex(graph, vid);
        assertEquals("foo", v.value("testKey"));

        Thread t = new Thread() {
            public void run() {
                Vertex v = getVertex(graph, vid);
                v.property("testKey", "bar");
                graph.tx().commit();
            }
        };
        
        t.start();
        t.join();
        
        assertEquals("foo", v.value("testKey"));

        try {
            v.property("testKey", "baz");
            graph.tx().commit();
            
            fail("Should throw concurrent mod exception");
        } catch (BitsyRetryException e) {
            // Ignore
        }
        
        v = getVertex(graph, vid);
        assertEquals("bar", v.value("testKey"));
        v.property("testKey", "baz");
        assertEquals("baz", v.value("testKey"));
        graph.tx().commit();
        
        v = getVertex(graph, vid);
        Vertex v2 = graph.addVertex();
        
        Edge e12 = v.addEdge("foo", v2);
        
        v.remove();
        removeVertex(graph, v2);
        
        try {
            v.remove();
            fail("Should throw exception");
        } catch (BitsyException e) {
            assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, e.getErrorCode());
        }
        
        try {
            e12.remove();
        } catch (BitsyException e) {
            assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, e.getErrorCode());
        } 
        
        try {
            v2.remove();
            fail("Should throw exception");
        } catch (BitsyException e) {
            assertEquals(BitsyErrorCodes.ELEMENT_ALREADY_DELETED, e.getErrorCode());
        }
        
        graph.tx().commit();
    }
    
    public void testLargeDegreePerformance() {
        long ts = System.currentTimeMillis();

        Vertex one = graph.addVertex();
        one.property("one", "1");
        Object oneId = one.id();

        int numVertices = 10000; // 1000000;
        Object[] vids = new Object[numVertices];
        for (int i = 0; i < numVertices; i++) { // Change to 1M for perf
            Vertex many = graph.addVertex();
            many.property("many", "2");
            addEdge(graph, one, many, "toMany");
            vids[i] = many.id();

            if (i % 1000 == 0) {
                System.out.println(i + " 1000 in " + (System.currentTimeMillis() - ts));
                ts = System.currentTimeMillis();

                graph.tx().commit();
                one = graph.vertices(oneId).next();
            }
        }
        
        for (int i=0; i < numVertices; i++) {
            Vertex v = getVertex(graph, vids[i]);

            Iterator<Edge> iter = v.edges(Direction.BOTH);
            assertTrue(iter.hasNext());
            iter.next();
            assertFalse(iter.hasNext());

            v.remove();

            if (i % 1000 == 0) {
                System.out.println(i + " 1000 in " + (System.currentTimeMillis() - ts));
                
                if (i % 5000 == 0) {
                    Iterator<Edge> iter2 = getVertex(graph, one.id()).edges(Direction.BOTH);
                    for (int j=0; j < numVertices - i - 1; j++) {
                        assertTrue(iter2.hasNext());
                        iter2.next();
                    }
                    assertFalse(iter.hasNext());
                }
                
                ts = System.currentTimeMillis();

                graph.tx().commit();
            }
        }

        graph.tx().commit();
    }
}
