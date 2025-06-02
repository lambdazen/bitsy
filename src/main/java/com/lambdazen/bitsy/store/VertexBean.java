package com.lambdazen.bitsy.store;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.Dictionary;
import java.io.Serializable;
import java.util.TreeMap;

public class VertexBean extends UUID implements Serializable {
    private static final long serialVersionUID = -2867517568410927192L;

    int version;
    String label;
    Dictionary properties;

    Object outEdges;
    Object inEdges;

    public VertexBean(UUID uuid, String label, Dictionary properties, int version) {
        super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());

        this.label = label;
        this.properties = properties;
        this.version = version;
    }

    /** Shallow copy constructor */
    public VertexBean(VertexBean orig) {
        super(orig.getMostSignificantBits(), orig.getLeastSignificantBits());

        this.label = orig.label;
        this.version = orig.version;
        this.properties = orig.properties;
        this.outEdges = orig.outEdges;
        this.inEdges = orig.inEdges;
    }

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

    @JsonProperty("l")
    public String getLabel() {
        return label;
    }

    public void copyFrom(VertexBean vBean) {
        this.version = vBean.version;
        this.properties = vBean.properties;
    }
}
