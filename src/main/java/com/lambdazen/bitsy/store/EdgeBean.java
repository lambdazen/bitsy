package com.lambdazen.bitsy.store;

import java.io.Serializable;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.lambdazen.bitsy.IEdge;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.Dictionary;

@JsonPropertyOrder({"class", "id", "v", "s", "o", "l", "i", "p"})
public class EdgeBean extends UUID implements IEdge, Serializable {
    private static final long serialVersionUID = -5962479601393604124L;
    
    Dictionary properties;
    String label;
    VertexBean outVertex;
    VertexBean inVertex;
    int version;
    
    public EdgeBean(UUID id, Dictionary properties, int version, String label, VertexBean outVertex, VertexBean inVertex) {
        super(id.getMostSignificantBits(), id.getLeastSignificantBits());

        this.properties = properties;
        this.version = version;
        this.label = label;
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }
    
    /** Shallow copy constructor */
    public EdgeBean(EdgeBean orig) {
        super(orig.getMostSignificantBits(), orig.getLeastSignificantBits());

        this.version = orig.version;
        this.properties = orig.properties;
        this.label = orig.label;
        this.outVertex = orig.outVertex;
        this.inVertex = orig.inVertex;
    }

 // Use UUID's toString()
//    public String toString() {
//        return "EdgeBean(id = " + getIdStr() + ", props " + getProperties() + ", version = " + version + ", label = " + label + ", outV = " + outVertex + ", inV = " + inVertex + ")";
//    }

    @JsonIgnore
    public UUID getId() {
        // I am the ID! Saves on object creation and equals checks. 
        return this;
    }
    
    @JsonProperty("id")
    public String getIdStr() {
        return uuidRepr();
    }

    @JsonProperty("p")
    public TreeMap<String, Object> getProperties() {
        if (properties == null) {
            return null;
        } else {
            TreeMap<String, Object> ans = new TreeMap<String, Object>();

            for (String key : properties.getPropertyKeys()) {
                ans.put(key, properties.getProperty(key));
            }

            return ans;
        }
    }
    
    @JsonIgnore
    public Dictionary getPropertiesDict() {
        return properties;
    }

    @JsonProperty("v")
    public int getVersion() {
        return version;
    }

    @JsonIgnore
    public UUID getInVertexId() {
        return inVertex.getId();
    }

    @JsonIgnore
    public UUID getOutVertexId() {
        return outVertex.getId();
    }

    @JsonProperty("l")
    public String getLabel() {
        return label;
    }
}
