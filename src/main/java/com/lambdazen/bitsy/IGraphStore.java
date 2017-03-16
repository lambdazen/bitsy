package com.lambdazen.bitsy;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.store.VertexBean;
import com.lambdazen.bitsy.tx.BitsyTransaction;

public interface IGraphStore {
    public void commit(ICommitChanges changes);

    /** Only to be used internally within the store */
    public VertexBean getVertex(UUID id);

    /** Returns a transaction-specific BitsyVertex given the tx and the ID */
    public BitsyVertex getBitsyVertex(BitsyTransaction tx, UUID id);

    /** Only to be used internally within the store */
    public EdgeBean getEdge(UUID id);
    
    /** Returns a transaction-specific BitsyEdge given the tx and the ID */
    public BitsyEdge getBitsyEdge(BitsyTransaction tx, UUID id);

    public List<EdgeBean> getEdges(UUID vertexId, Direction dir, String[] edgeLabels);
    
    public Collection<VertexBean> getAllVertices();
    
    public Collection<EdgeBean> getAllEdges();
    
    public <T extends Element> void createKeyIndex(String key, Class<T> elementType);
    
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementType);
    
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementType);
    
    public void shutdown();

    public Collection<VertexBean> lookupVertices(String key, Object value);
    
    public Collection<EdgeBean> lookupEdges(String key, Object value);

    public boolean allowFullGraphScans();
}
