package com.lambdazen.bitsy.wrapper;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BitsyAutoReloadingVertex implements Vertex {
    BitsyVertex vertex;
    BitsyGraph graph;

    public BitsyAutoReloadingVertex(BitsyGraph g, BitsyVertex v) {
        this.vertex = v;
        this.graph = g;
    }

    public Vertex getBaseVertex() {
        if (((BitsyTransaction) vertex.getTransaction()).isStopped()) {
            vertex = (BitsyVertex) graph.vertices(vertex.id()).next();
        }

        return vertex;
    }

    @Override
    public Object id() {
        // Don't reload just for the ID
        return vertex.id();
    }

    @Override
    public String label() {
        return getBaseVertex().label();
    }

    public int hashCode() {
        return getBaseVertex().hashCode();
    }

    public boolean equals(Object o) {
        return getBaseVertex().equals(o);
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        getBaseVertex().remove();
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return new BitsyAutoReloadingEdge(graph, (BitsyEdge) (getBaseVertex().addEdge(label, inVertex, keyValues)));
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... labels) {
        return new BitsyAutoReloadingGraph.EdgeIterator(graph, getBaseVertex().edges(direction, labels));
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... labels) {
        return new BitsyAutoReloadingGraph.VertexIterator(graph, getBaseVertex().vertices(direction, labels));
    }

    @Override
    public <T> VertexProperty<T> property(String key) {
        return getBaseVertex().property(key);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        return getBaseVertex().property(key, value);
    }

    @Override
    public <V> VertexProperty<V> property(Cardinality cardinality, String key, V value, Object... keyValues) {
        return getBaseVertex().property(cardinality, key, value, keyValues);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return getBaseVertex().properties(propertyKeys);
    }
}
