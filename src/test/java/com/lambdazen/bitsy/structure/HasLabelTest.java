package com.lambdazen.bitsy.structure;

import com.lambdazen.bitsy.BitsyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

public class HasLabelTest {
    @Test
    public void bitsyPredicateTest() {
        Graph g = new BitsyGraph();

        final List<Object> props1 = new ArrayList<>();
        props1.add(T.label);
        props1.add("vert1");
        props1.add("p1");
        props1.add(1);
        props1.add("p2");
        props1.add("2");
        g.addVertex(props1.toArray());

        final List<Object> props2 = new ArrayList<>();
        props2.add(T.label);
        props2.add("vert2");
        props2.add("2p1");
        props2.add(1);
        props2.add("2p2");
        props2.add("2");
        Object vert2 = g.addVertex(props2.toArray());

        assertTrue(g.traversal().V().hasLabel(P.within("vert2")).next().equals(vert2));
    }
}
