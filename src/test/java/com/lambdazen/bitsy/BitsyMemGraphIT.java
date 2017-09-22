package com.lambdazen.bitsy;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.lambdazen.bitsy.store.Record;
import com.lambdazen.bitsy.store.Record.RecordType;

public class BitsyMemGraphIT extends BitsyGraphIT
{
    public boolean isPersistent() {
        return false;
    }
    
    public void setUp() {
        System.out.println("Setting up memory-only graph");
        graph = new BitsyGraph(false);
    }
    
    public void tearDown() {
        System.out.println("Tearing down graph");
        try {
        	graph.close();
        } catch (Exception e) {
        	throw new RuntimeException("Couldn't close", e);
        }
    }

    public void testPersistence() {
        // Disable
    }
    
    public void testObsolescence() {
        IGraphStore store = ((BitsyGraph)graph).getStore();
        
        // Create a vertex
        Vertex v = graph.addVertex();
        Object vid = v.id();
        v.property("foo", "bar");
        
        // Self edge
        Edge e = v.addEdge("self", v);
        Object eid = e.id();
        
        graph.tx().commit();

        Record v1MRec = new Record(RecordType.V, "{\"id\":\"" + vid + "\",\"v\":1,\"s\":\"M\"}");
        assertFalse(v1MRec.checkObsolete(store, false, 1, null));
        assertFalse(v1MRec.checkObsolete(store, true, 1, null));

        Record e1MRec = new Record(RecordType.E, "{\"id\":\"" + eid + "\",\"v\":1,\"s\":\"M\",\"o\":\"" + vid + "\",\"l\":\"" + vid + "\",\"i\":\"" + vid + "\"}");
        assertFalse(e1MRec.checkObsolete(store, false, 1, null));
        assertFalse(e1MRec.checkObsolete(store, true, 1, null));

        // Create a vertex
        v = graph.vertices(vid).next();
        v.property("foo", "baz");

        e = v.edges(Direction.IN, "self").next();
        e.property("foo", "baz");

        graph.tx().commit();

        Record v2MRec = new Record(RecordType.V, "{\"id\":\"" + vid + "\",\"v\":2,\"s\":\"M\"}");
        Record v1DRec = new Record(RecordType.V, "{\"id\":\"" + vid + "\",\"v\":1,\"s\":\"D\"}");
        
        assertTrue(v1MRec.checkObsolete(store, false, 1, null));
        assertTrue(v1MRec.checkObsolete(store, true, 1, null));

        assertFalse(v1DRec.checkObsolete(store, false, 1, null));
        assertTrue(v1DRec.checkObsolete(store, true, 1, null));

        assertFalse(v2MRec.checkObsolete(store, false, 1, null));
        assertFalse(v2MRec.checkObsolete(store, true, 1, null));

        Record e2MRec = new Record(RecordType.E, "{\"id\":\"" + eid + "\",\"v\":2,\"s\":\"M\",\"o\":\"" + vid + "\",\"l\":\"" + vid + "\",\"i\":\"" + vid + "\"}");
        Record e1DRec = new Record(RecordType.E, "{\"id\":\"" + eid + "\",\"v\":1,\"s\":\"D\",\"o\":\"" + vid + "\",\"l\":\"" + vid + "\",\"i\":\"" + vid + "\"}");
        
        assertTrue(e1MRec.checkObsolete(store, false, 1, null));
        assertTrue(e1MRec.checkObsolete(store, true, 1, null));

        assertFalse(e1DRec.checkObsolete(store, false, 1, null));
        assertTrue(e1DRec.checkObsolete(store, true, 1, null));

        assertFalse(e2MRec.checkObsolete(store, false, 1, null));
        assertFalse(e2MRec.checkObsolete(store, true, 1, null));

        // Delete vertex
        v = graph.vertices(vid).next();
        v.remove();

        // Edge will get deleted automatically!
        
        graph.tx().commit();
        
        Record v2DRec = new Record(RecordType.V, "{\"id\":\"" + vid + "\",\"v\":1,\"s\":\"D\"}");
        assertFalse(v2DRec.checkObsolete(store, false, 1, null));
        assertTrue(v2DRec.checkObsolete(store, true, 1, null));
    }

}
