package com.lambdazen.bitsy;

import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.store.IStringCanonicalizer;
import com.lambdazen.bitsy.store.VertexBean;
import com.lambdazen.bitsy.store.VertexBeanJson;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BitsyVertex extends BitsyElement implements Vertex {
    private static final Direction[] directions = new Direction[] {Direction.OUT, Direction.IN};

    public BitsyVertex(
            UUID id, String label, Dictionary properties, BitsyTransaction tx, BitsyState state, int version) {
        super(id, label, properties, tx, state, version);
    }

    public BitsyVertex(VertexBean bean, BitsyTransaction tx, BitsyState state) {
        this(bean.getId(), bean.getLabel(), bean.getPropertiesDict(), tx, state, bean.getVersion());
    }

    @Override
    public String label() {
        String result = super.label();
        return (result == null) ? Vertex.DEFAULT_LABEL : result;
    }

    @Override
    public Iterator<Edge> edges(Direction dir, String... edgeLabels) {
        return tx.getEdges(this, dir, edgeLabels).iterator();
    }

    public VertexBean asBean() {
        // The TX is usually not active at this point. So no checks.
        return new VertexBean((UUID) id, label, properties, version);
    }

    public VertexBean asBean(IStringCanonicalizer canonicalizer) {
        if (properties != null) {
            properties.canonicalizeKeys(canonicalizer);
        }

        return asBean();
    }

    public VertexBeanJson asJsonBean() {
        // The TX is usually not active at this point. So no checks.
        // TreeMap<String, Object> propertyMap = (properties == null) ? null : properties.toMap();
        return new VertexBeanJson((UUID) id, label, properties, version, state);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction dir, String... edgeLabels) {
        final ArrayList<Vertex> vertices = new ArrayList<Vertex>();

        for (Direction myDir : directions) {
            if ((myDir == dir) || (dir == Direction.BOTH)) {
                Iterator<Edge> iter = edges(myDir, edgeLabels);
                while (iter.hasNext()) {
                    Edge e = iter.next();
                    Vertex toAdd = (myDir.opposite() == Direction.IN) ? e.inVertex() : e.outVertex();
                    vertices.add(toAdd);
                }
            }
        }

        // Go through the edges and load the vertices
        return vertices.iterator();
    }

    public void incrementVersion() {
        // It is OK for the version to wrap around MAX_INT
        this.version++;
    }

    public void remove() {
        tx.removeVertex(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (keyValues.length % 2 == 1) {
            throw new IllegalArgumentException(
                    "Expecting even number of items in the keyValue array. Found " + keyValues.length);
        }

        if (label == null) {
            throw new IllegalArgumentException("You have to specify a non-null String label when adding an edge");
        } else if (label.length() == 0) {
            throw new IllegalArgumentException("You have to specify a non-empty String label when adding an edge");
        } else if (label.charAt(0) == '~') {
            throw new IllegalArgumentException("Labels beginning with ~ are invalid");
        }

        if (inVertex == null) {
            throw new IllegalArgumentException("The inVertex supplied to addEdge() is null");
        }

        // Validate first
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i] == T.label) {
                throw new UnsupportedOperationException("Encountered T.label in addVertex");
            } else if (keyValues[i] == T.id) {
                throw new UnsupportedOperationException(
                        "Encountered T.id in addVertex", new BitsyException(BitsyErrorCodes.NO_CUSTOM_ID_SUPPORT));
            } else if (keyValues[i] == null) {
                throw new IllegalArgumentException("Encountered a null key in argument #" + i);
            } else if (keyValues[i + 1] == null) {
                throw new IllegalArgumentException("Encountered a null value in argument #" + i);
            } else if (!(keyValues[i] instanceof String)) {
                throw new IllegalArgumentException(
                        "Encountered a non-string key: " + keyValues[i] + " in argument #" + i);
            }
        }

        // Construct the edge with this as the out vertex
        BitsyEdge edge =
                new BitsyEdge(UUID.randomUUID(), null, tx, BitsyState.M, 0, label, (UUID) id(), (UUID) inVertex.id());

        for (int i = 0; i < keyValues.length; i = i + 2) {
            String key = (String) keyValues[i];
            edge.property(key, keyValues[i + 1]);
        }

        tx.addEdge(edge);

        return edge;
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    // THERE ARE TWO MORE COPIES OF THIS CODE IN ELEMENT AND EDGE
    @Override
    public <T> VertexProperty<T> property(String key) {
        T value = value(key);
        if (value == null) {
            return VertexProperty.<T>empty();
        } else {
            return new BitsyVertexProperty<T>(this, key, value);
        }
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        super.property(key, value);
        return new BitsyVertexProperty<V>(this, key, value);
    }

    @Override
    public <V> VertexProperty<V> property(
            final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        if (cardinality != Cardinality.single) {
            // For some reason, TP3 tests fail with this exception
            // throw new BitsyException(BitsyErrorCodes.NO_MULTI_PROPERTY_SUPPORT, "Encountered cardinality: " +
            // cardinality.toString());
        } else if (keyValues.length != 0) {
            throw new UnsupportedOperationException(
                    "Encountered key values: " + keyValues.toString(),
                    new BitsyException(BitsyErrorCodes.NO_META_PROPERTY_SUPPORT));
        }

        return property(key, value);
    }

    // THERE ARE TWO MORE COPIES OF THIS CODE IN ELEMENT AND EDGE
    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        ArrayList<VertexProperty<V>> ans = new ArrayList<VertexProperty<V>>();

        if (propertyKeys.length == 0) {
            if (this.properties == null) return Collections.emptyIterator();
            propertyKeys = this.properties.getPropertyKeys();
        }

        for (String key : propertyKeys) {
            VertexProperty<V> prop = property(key);
            if (prop.isPresent()) ans.add(prop);
        }
        return ans.iterator();
    }
}
