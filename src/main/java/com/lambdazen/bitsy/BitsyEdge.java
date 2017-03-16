package com.lambdazen.bitsy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.store.EdgeBeanJson;
import com.lambdazen.bitsy.tx.BitsyTransaction;

public class BitsyEdge extends BitsyElement implements Edge, IEdge {
    UUID outVertexId;
    UUID inVertexId;
    
    public BitsyEdge(UUID id, Dictionary properties, BitsyTransaction tx, BitsyState state, int version, String label, UUID outVertexId, UUID inVertexId) {
        super(id, label, properties, tx, state, version);

        if (label == null) {
            throw new IllegalArgumentException("Edge label can not be null"); // Enforced by 2.3.0 test case
        }
        
        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
    }

    public BitsyEdge(EdgeBean bean, BitsyTransaction tx, BitsyState state) {
        this(bean.getId(), bean.getPropertiesDict(), 
                tx, state, bean.getVersion(), bean.getLabel(), bean.getOutVertexId(), bean.getInVertexId());
    }

    public EdgeBeanJson asJsonBean() {
        // The TX is usually not active at this point. So no checks.
        return new EdgeBeanJson((UUID)id, properties, version, label, outVertexId, inVertexId, state);
    }

    @Override
    public Iterator<Vertex> vertices(Direction dir) {
        tx.validateForQuery(this);

        if (dir != Direction.BOTH) {
            Vertex ans = inOrOutVertex(dir);

            return Collections.singleton(ans).iterator();
        } else {
            return bothVertices();
        }
    }

    @Override
    public Vertex inVertex() {
        tx.validateForQuery(this);

        return inOrOutVertex(Direction.IN);
    }

    @Override
    public Vertex outVertex() {
        tx.validateForQuery(this);

        return inOrOutVertex(Direction.OUT);
    }

    @Override
    public Iterator<Vertex> bothVertices() {
        tx.validateForQuery(this);

        Vertex inV = inVertex();
        Vertex outV = outVertex();
        return Arrays.asList(new Vertex[] {outV, inV}).iterator();
    }

    private Vertex inOrOutVertex(Direction dir) {
        Vertex ans = tx.getVertex(getVertexId(dir));

        // Vertex may disappear in READ_COMMITTED MODE
        if (ans == null) {
            throw new BitsyRetryException(BitsyErrorCodes.CONCURRENT_MODIFICATION, "The vertex in direction " + dir + " of the edge " + this.id() + " was removed by another transaction");
        }

        return ans;
    }

    public UUID getInVertexId() {
        return inVertexId;
    }

    public UUID getOutVertexId() {
        return outVertexId;
    }

    public UUID getVertexId(Direction dir) {
        if (dir == Direction.IN) {
            return inVertexId;
        } else if (dir == Direction.OUT) {
            return outVertexId;
        } else {
            throw new IllegalArgumentException("Unsupported direction " + dir);
        }
    }

    public void incrementVersion() {
        this.version++;
    }
    
    public void remove() {
        tx.removeEdge(this);
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    // THERE ARE TWO MORE COPIES OF THIS CODE IN ELEMENT AND VERTEX
    public <T> Iterator<Property<T>> properties(String... propertyKeys) {
        ArrayList<Property<T>> ans = new ArrayList<Property<T>>();

        if (propertyKeys.length == 0) {
        	if (this.properties == null) return Collections.emptyIterator();
        	propertyKeys = this.properties.getPropertyKeys();
        }

        for (String key : propertyKeys) {
            Property<T> prop = property(key);
            if (prop.isPresent()) ans.add(prop);
        }
        return ans.iterator();
    }
}
