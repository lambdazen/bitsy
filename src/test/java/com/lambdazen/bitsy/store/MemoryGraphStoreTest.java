package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.ICommitChanges;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.DictionaryFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import junit.framework.TestCase;
import org.apache.tinkerpop.gremlin.structure.Direction;

public class MemoryGraphStoreTest extends TestCase {
    MemoryGraphStore store;

    public MemoryGraphStoreTest() {}

    protected void setUp() {
        System.out.println("Making DB");
        this.store = new MemoryGraphStore(true);
        System.out.println("Made DB");
    }

    protected void tearDown() {
        System.out.println("Close");
    }

    //    public void testOpenClose() {
    //        System.out.println("Opened DB");
    //    }

    public void testSomething() {
        Map foo = new TreeMap();
        foo.put(1, 2);
        foo.put(2, 3);

        assert (foo.values().size() == 2);
        assert (foo.values().size() == 2);
    }

    public void testVertexInserts() {
        int numCommits = 100;
        int numPerCommit = 100;

        List<UUID> uuids = new ArrayList<UUID>();
        long ts = System.currentTimeMillis();
        for (int i = 0; i < numCommits * numPerCommit; i += numPerCommit) {
            if (i % 1000 == 0) System.out.println("Inserting vertex " + i + " to " + (i + numPerCommit));

            VertexCommitChanges changes = new VertexCommitChanges(i, numPerCommit);
            store.commit(changes);

            uuids.addAll(changes.getVertexIDs());
        }
        System.out.println("Took " + (System.currentTimeMillis() - ts) + " ms to commit " + (numPerCommit * numCommits)
                + " vertices");

        ts = System.currentTimeMillis();
        // Check to see if the stuff made it in
        int counter = 0;
        for (UUID uuid : uuids) {
            // System.out.println("Found " + uuid);
            VertexBean bean = store.getVertex(uuid);
            assertNotNull(bean);
            assertEquals(1, bean.getVersion());
            assertEquals(counter++, bean.getProperties().get("Vertex"));
        }
        System.out.println("Took " + (System.currentTimeMillis() - ts) + " ms to query " + uuids.size() + " vertices");
    }

    public void testBipartiteGraph() {
        for (boolean reverse : new boolean[] {false, true}) {
            for (boolean useEdgeLabels : new boolean[] {true, false}) {
                System.out.println(
                        "Entering iteration with reverse " + reverse + ", and useEdgeLabels " + useEdgeLabels);

                int numCommits = 100;
                int numPerCommit = 10;
                int partSize = numCommits * numPerCommit;

                UUID[] outVertices = createVertices(numPerCommit, partSize).toArray(new UUID[0]);
                UUID[] inVertices = createVertices(numPerCommit, partSize).toArray(new UUID[0]);
                EdgeCommitChanges ecc = new EdgeCommitChanges();

                // Now add edges from one to the other
                long ts = System.currentTimeMillis();
                for (int i = 0; i < outVertices.length; i++) {
                    UUID[] ins = new UUID[] {
                        inVertices[(5 * i + 1) % partSize],
                        inVertices[(5 * i + 4) % partSize],
                        inVertices[(5 * i + 7) % partSize]
                    };

                    for (int j = 0; j < ins.length; j++) {
                        String label = "";

                        if (useEdgeLabels) {
                            label = "Label " + j;
                        }

                        if (!reverse) {
                            ecc.addEdge(outVertices[i], label, ins[j]);
                        } else {
                            ecc.addEdge(ins[j], label, outVertices[i]);
                        }
                    }

                    if (i % numPerCommit == 0) {
                        // Commit now
                        int size = ecc.size();
                        store.commit(ecc);
                        ecc = new EdgeCommitChanges();

                        // double duration = (System.currentTimeMillis() - ts);
                        // System.out.println("Took " + duration + " ms to save " + size + " edges. Rate = " + (duration
                        // / size) + " ms per edge");

                        // ts = System.currentTimeMillis();
                    }
                }
                // Commit before the next stage
                store.commit(ecc);

                // Now check to see if the edges go to the right places
                ts = System.currentTimeMillis();
                for (int i = 0; i < outVertices.length; i++) {
                    UUID[] ins = new UUID[] {
                        inVertices[(5 * i + 1) % partSize],
                        inVertices[(5 * i + 4) % partSize],
                        inVertices[(5 * i + 7) % partSize]
                    };

                    List<EdgeBean> edgeList;
                    if (useEdgeLabels && Math.random() < 0.2) {
                        // System.out.println("Explicit label query");
                        edgeList = store.getEdges(outVertices[i], reverse ? Direction.IN : Direction.OUT, new String[] {
                            "Label 0", "Label 1", "Label 2"
                        });
                    } else {
                        // System.out.println("Null label query");
                        edgeList = store.getEdges(outVertices[i], reverse ? Direction.IN : Direction.OUT, null);
                    }

                    if (edgeList.size() < 3) {
                        // Something is wrong
                        System.out.println("Edges retrieved " + edgeList);
                        System.out.println("Vertices expected: ");
                        for (int k = 0; k < 3; k++) {
                            System.out.print(ins[k] + " ");
                        }
                        fail("WRONG edgeList size");
                    }

                    for (int j = 0; j < 3; j++) {
                        EdgeBean edge = edgeList.get(j);
                        assertEquals(outVertices[i], reverse ? edge.getInVertexId() : edge.getOutVertexId());
                        // Either foo is missing, or it is bar -- poor man's validation
                        assertTrue(edge.getProperties() == null
                                || "bar".equals(edge.getProperties().get("foo")));

                        boolean match = false;
                        for (int k = 0; k < 3; k++) {
                            UUID otherVertexId = reverse ? edge.getOutVertexId() : edge.getInVertexId();
                            if (otherVertexId.equals(ins[k])) {
                                // Match
                                match = true;

                                if (useEdgeLabels) {
                                    String label = "Label " + k;
                                    assertEquals(label, edge.getLabel());

                                    // Additional check to make sure that the edge can be retrieved by label
                                    if (Math.random() < 0.2) {
                                        List<EdgeBean> edgeByLabelList = store.getEdges(
                                                outVertices[i],
                                                reverse ? Direction.IN : Direction.OUT,
                                                new String[] {label});
                                        assertEquals(1, edgeByLabelList.size());
                                        assertEquals(
                                                edge.getId(),
                                                edgeByLabelList.get(0).getId());
                                    }
                                } else {
                                    assertEquals("", edge.getLabel());
                                }
                            }
                        }

                        assertTrue(match);
                    }
                }

                double duration = (System.currentTimeMillis() - ts);
                System.out.println("Took " + duration + " ms to save " + (numCommits * numPerCommit * 3)
                        + " edges. Rate = " + (duration / (numCommits * numPerCommit * 3)) + " ms per edge");

                // Now, remove all the vertices to make sure that the edges are also gone
                ts = System.currentTimeMillis();
                deleteVertices(inVertices, numPerCommit);
                duration = (System.currentTimeMillis() - ts);
                System.out.println("Took " + duration + " ms to delete " + inVertices.length
                        + " vertices with 3 incident edges. Rate = " + (duration / inVertices.length)
                        + " ms per vertex");

                for (VertexBean vBean : store.getAllVertices()) {
                    assertNull(vBean.outEdges);
                    assertNull(vBean.inEdges);
                }

                // Check if there are no edges into inVertices
                ts = System.currentTimeMillis();
                for (int i = 0; i < outVertices.length; i++) {
                    List<EdgeBean> edgeList =
                            store.getEdges(outVertices[i], reverse ? Direction.IN : Direction.OUT, null);
                    assertEquals(0, edgeList.size());
                }
            }
        }
    }

    private List<UUID> createVertices(int numPerCommit, int partSize) {
        List<UUID> ans = new ArrayList<UUID>();

        for (int i = 0; i < partSize; i += numPerCommit) {
            if (i % 1000 == 0) System.out.println("Inserting vertex " + i + " to " + (i + numPerCommit));

            VertexCommitChanges changes = new VertexCommitChanges(i, numPerCommit);
            store.commit(changes);
            ans.addAll(changes.getVertexIDs());
        }

        return ans;
    }

    private void deleteVertices(UUID[] toDelete, int numPerCommit) {
        int partSize = toDelete.length;
        for (int i = 0; i < partSize; i += numPerCommit) {
            // System.out.println("Deleting vertex from #" + i);

            VertexCommitChanges changes = new VertexCommitChanges(i, numPerCommit, toDelete, 1);
            store.commit(changes);
        }
    }

    public class VertexCommitChanges implements ICommitChanges {
        List<BitsyVertex> vertices;

        int start, num;
        List<UUID> uuids;
        UUID[] toDelete;
        int version;

        public VertexCommitChanges(int start, int num) {
            this(start, num, null);
        }

        public VertexCommitChanges(int start, int num, UUID[] toDelete) {
            this(start, num, toDelete, 0);
        }

        public VertexCommitChanges(int start, int num, UUID[] toDelete, int version) {
            this.start = start;
            this.num = num;
            this.toDelete = toDelete;
            this.uuids = new ArrayList<UUID>();
            this.version = version;
        }

        public List<UUID> getVertexIDs() {
            return uuids;
        }

        @Override
        public Collection<BitsyVertex> getVertexChanges() {
            if (vertices == null) {
                vertices = new ArrayList<BitsyVertex>();

                for (int i = start; i < start + num; i++) {
                    Map<String, Object> propMap = new TreeMap<String, Object>();
                    propMap.put("Vertex", i);

                    UUID uuid;
                    BitsyState state;
                    if (toDelete == null) {
                        uuid = UUID.randomUUID();
                        state = BitsyState.M;
                    } else {
                        uuid = toDelete[i];
                        state = BitsyState.D;
                    }

                    // Insert or delete a vertex
                    // vertices.add(new BitsyVertex(uuid, DictionaryFactory.fromMap(propMap), null, state, version));
                    vertices.add(new BitsyVertex(uuid, null, DictionaryFactory.fromMap(propMap), null, state, version));

                    uuids.add(uuid);

                    if (i % 97 == 0) {
                        // Place some random unmodified nodes with the same UUID. These should be ignored in the commit.
                        vertices.add(new BitsyVertex(uuid, null, null, null, BitsyState.U, 23));
                    }
                }
            }

            return vertices;
        }

        @Override
        public Collection<BitsyEdge> getEdgeChanges() {
            return Collections.emptyList();
        }
    }

    public class EdgeCommitChanges implements ICommitChanges {
        List<UUID> outUUIDs;
        List<UUID> inUUIDs;
        List<String> edgeLabels;
        List<UUID> edgeUUIDs;
        List<BitsyEdge> edges;

        public EdgeCommitChanges() {
            this.outUUIDs = new ArrayList<UUID>();
            this.inUUIDs = new ArrayList<UUID>();
            this.edgeLabels = new ArrayList<String>();
        }

        public int size() {
            return outUUIDs.size();
        }

        public void addEdge(UUID outUUID, String label, UUID inUUID) {
            outUUIDs.add(outUUID);
            edgeLabels.add(label);
            inUUIDs.add(inUUID);
        }

        @Override
        public Collection<BitsyVertex> getVertexChanges() {
            return Collections.emptyList();
        }

        @Override
        public Collection<BitsyEdge> getEdgeChanges() {
            if (edges == null) {
                this.edgeUUIDs = new ArrayList<UUID>(outUUIDs.size());
                this.edges = new ArrayList<BitsyEdge>(outUUIDs.size());

                Iterator<UUID> outIter = outUUIDs.iterator();
                Iterator<String> edgeIter = edgeLabels.iterator();
                Iterator<UUID> inIter = inUUIDs.iterator();

                while (outIter.hasNext()) {
                    UUID outV = outIter.next();
                    UUID inV = inIter.next();
                    String label = edgeIter.next();
                    Map<String, Object> edgeProps = null;
                    if (Math.random() < 0.5) {
                        edgeProps = new TreeMap<String, Object>();
                        edgeProps.put("foo", "bar");
                    }

                    UUID edgeUUID = UUID.randomUUID();
                    edgeUUIDs.add(edgeUUID);

                    BitsyEdge edge = new BitsyEdge(
                            edgeUUID,
                            DictionaryFactory.fromMap(edgeProps),
                            null,
                            BitsyState.M,
                            Integer.MIN_VALUE,
                            label,
                            outV,
                            inV);
                    edges.add(edge);

                    if (Math.random() < 0.01) {
                        // Place some random unmodified nodes with the same UUID.
                        // These should be ignored in the commit. Keeping
                        // probability at 5% to not affect ms/edge too much
                        edges.add(
                                new BitsyEdge(edgeUUID, null, null, BitsyState.U, Integer.MIN_VALUE, label, outV, inV));
                    }

                    // Once in a while, create an edge called "Invalid" and remove
                    // it. If there are issues with the delete logic, it will break
                    // subsequent asserts. Keeping probability at 5% to not affect ms/edge too much
                    if (Math.random() < 0.05) {
                        // Place some random unmodified nodes with the same UUID. These should be ignored in the commit.
                        UUID deleteEdgeId = UUID.randomUUID();

                        edges.add(new BitsyEdge(
                                deleteEdgeId,
                                DictionaryFactory.fromMap(edgeProps),
                                null,
                                BitsyState.M,
                                1,
                                "To be removed",
                                outV,
                                inV));

                        // Now remove it
                        edges.add(new BitsyEdge(
                                deleteEdgeId,
                                DictionaryFactory.fromMap(edgeProps),
                                null,
                                BitsyState.D,
                                2,
                                "To be removed",
                                outV,
                                inV));
                    }
                }
            }

            return edges;
        }
    }
}
