package com.lambdazen.bitsy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.ads.dict.Dictionary1;
import com.lambdazen.bitsy.tx.BitsyTransaction;

public abstract class BitsyElement implements Element {
	public static enum PropType {ELEMENT, VERTEX, EDGE}; 

    Object id;
    String label;
    Dictionary properties;
    BitsyTransaction tx;
    BitsyState state;
    int version;
    boolean updated;
    
    public BitsyElement(Object id, String label, Dictionary properties, BitsyTransaction tx, BitsyState state, int version) {
        this.id = id;
        this.label = label;
        this.properties = properties;
        this.tx = tx;
        this.state = state;
        this.version = version;
        this.updated = false;
    }

    @Override
    public Object id() {
        // No TX check to return the ID
        return id;
    }

    @Override
    public String label() {
        // There is no Tx validation for label because it is used even after deletion to update indexes, etc. 
        return label;
    }

    @Override
    public Graph graph() {
        return tx.graph();
    }
    
    public Dictionary getPropertyDict() {
        return properties;
    }

    @Override
    public <T>T value(String key) {
        tx.validateForQuery(this);

        if (properties == null) {
            return null;
        } else {
            return (T) (properties.getProperty(key));
        }
    }

    @Override
    public Set<String> keys() {
        tx.validateForQuery(this);
        
        if (properties == null) {
            return Collections.emptySet();
        } else {
            return new CopyOnWriteArraySet<String>(Arrays.asList(properties.getPropertyKeys()));
        }
    }

    public <T>T removeProperty(String key) {
        markForUpdate();

        if (properties == null) {
        	return null;
        } else {
        	Object ans = properties.getProperty(key);

        	properties = properties.removeProperty(key);

        	return (T)ans;
        }
    }

    @Override
    public <T> Property<T> property(String key, T value) {
        if (value == null) {
            throw new IllegalArgumentException("A null property can not be stored. You can call removeProperty() instead");
        }

        markForUpdate();

        if (key == null) {
        	throw new IllegalArgumentException("Expecting non-null key in setProperty");
        } else if (key.length() == 0) {
        	throw new IllegalArgumentException("Expecting non-empty key in setProperty");
        } else if (key.equals("id")) {
        	throw new IllegalArgumentException("Can not set the 'id' property on an element");
        } else if (key.equals("label")) {
        	throw new IllegalArgumentException("Can not set the 'label' property on an element");
        } else if (key.charAt(0) == '~') {
        	throw new IllegalArgumentException("Can not set a property beginning with ~ on an element");
        }

        if (this.properties == null) {
        	this.properties = new Dictionary1(key, value);
        } else {
        	this.properties = properties.setProperty(key, value);
        }

        assert (properties != null);

        return new BitsyProperty<T>(this, key, value);
    }

    // WARNING: THERE IS ONE MORE COPY OF THIS CODE IN VERTEX
    @Override
    public <T> Property<T> property(String key) {
        T value = value(key);
        if (value == null) {
            return Property.<T>empty();
        } else {
            return new BitsyProperty<T>(this, key, value);
        }
    }

    // WARNING: THERE ARE TWO MORE COPIES OF THIS CODE IN VERTEX AND EDGE
    @Override
    public <T> Iterator<? extends Property<T>> properties(String... propertyKeys) {
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

    /** This method prepares the vertex/edge for an update */
    public void markForUpdate() {
        if (!updated) {
            updated = true;
            
            // Make a copy of the underlying property map, if non-null
            if (properties != null) {
                properties = properties.copyOf();
            }

            tx.markForPropertyUpdate(this);
        }
    }

    @Override
    public abstract void remove();

    public BitsyState getState() {
        return state;
    }
    
    public void setState(BitsyState state) {
        this.state = state;
    }

    public int getVersion() {
        return version;
    }

    public ITransaction getTransaction() {
        return tx;
    }

    // Moved to hashCode and equals from ElementHelper -- to pass Gremlin tests
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(Object object) {
    	return ElementHelper.areEqual(this, object);
    }
}