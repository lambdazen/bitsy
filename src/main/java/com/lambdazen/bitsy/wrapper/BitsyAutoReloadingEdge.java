package com.lambdazen.bitsy.wrapper;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BitsyAutoReloadingEdge implements Edge {
    BitsyEdge edge;
    BitsyGraph graph;

    public BitsyAutoReloadingEdge(BitsyGraph g, BitsyEdge e) {
        this.edge = e;
        this.graph = g;
    }

    public Edge getBaseEdge() {
        if (((BitsyTransaction) edge.getTransaction()).isStopped()) {
            edge = (BitsyEdge) graph.edges(edge.id()).next();
        }

        return edge;
    }

    @Override
    public void remove() {
        getBaseEdge().remove();
    }

    @Override
    public Object id() {
        // Don't reload just for the ID
        return edge.id();
    }

    @Override
    public String label() {
        return getBaseEdge().label();
    }

    public int hashCode() {
        return getBaseEdge().hashCode();
    }

    public boolean equals(Object o) {
        return getBaseEdge().equals(o);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public Graph graph() {
        return getBaseEdge().graph();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return getBaseEdge().property(key, value);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return new BitsyAutoReloadingGraph.VertexIterator(graph, getBaseEdge().vertices(direction));
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return getBaseEdge().properties(propertyKeys);
    }
}
