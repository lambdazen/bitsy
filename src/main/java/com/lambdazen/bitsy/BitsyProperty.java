package com.lambdazen.bitsy;

import java.util.NoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BitsyProperty<T> implements Property<T> {
    BitsyElement element;
    String key;
    T value;
    boolean removed = false;

    public BitsyProperty(BitsyElement element, String key, T value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public T value() throws NoSuchElementException {
    	if (removed) {
    		throw new NoSuchElementException("This property is empty");
    	} else {
    		return value;
    	}
    }

    @Override
    public boolean isPresent() {
        return !removed;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {
    	if (isPresent()) {
    		element.removeProperty(key);
    		this.removed = true;
    	}
    }

    // Moved to ElementHelper hashCode and equals in TP3
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }
}