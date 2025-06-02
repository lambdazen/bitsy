package com.lambdazen.bitsy;

import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface ITransaction extends Transaction {
    public void save(boolean commit);

    public void validateForQuery(BitsyElement bitsyElement) throws BitsyException;

    public Vertex getVertex(UUID outVertexId) throws BitsyException;

    public Edge getEdge(UUID id) throws BitsyException;

    public Iterable<Edge> getEdges(BitsyVertex bitsyVertex, Direction dir, String... edgeLabels) throws BitsyException;

    public void markForPropertyUpdate(BitsyElement bitsyElement) throws BitsyException;

    public void addVertex(BitsyVertex vertex) throws BitsyException;

    public void removeVertex(BitsyVertex vertex) throws BitsyException;

    public void addEdge(BitsyEdge edge) throws BitsyException;

    public void removeEdge(BitsyEdge edge) throws BitsyException;

    public Iterator<Vertex> getAllVertices();

    public Iterator<Edge> getAllEdges();

    public Iterator<BitsyVertex> lookupVertices(String key, Object value);

    public Iterator<BitsyEdge> lookupEdges(String key, Object value);

    public BitsyIsolationLevel getIsolationLevel();

    public void setIsolationLevel(BitsyIsolationLevel level);
}
