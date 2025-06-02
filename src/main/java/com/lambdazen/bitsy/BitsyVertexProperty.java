package com.lambdazen.bitsy;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BitsyVertexProperty<V> extends BitsyProperty<V> implements VertexProperty<V> {
    public BitsyVertexProperty(final BitsyVertex vertex, final String key, final V value) {
        super(vertex, key, value);
    }

    @Override
    public Set<String> keys() {
        return Collections.emptySet();
    }

    @Override
    public <U> Property<U> property(final String key) {
        throw new BitsyException(BitsyErrorCodes.NO_META_PROPERTY_SUPPORT);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        throw new BitsyException(BitsyErrorCodes.NO_META_PROPERTY_SUPPORT);
    }

    @Override
    public Vertex element() {
        return (BitsyVertex) super.element();
    }

    @Override
    public <U> Iterator<Property<U>> properties(final String... propertyKeys) {
        throw new BitsyException(BitsyErrorCodes.NO_META_PROPERTY_SUPPORT);
    }

    @Override
    public Object id() {
        return element().id().toString() + ":" + key();
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }
}
