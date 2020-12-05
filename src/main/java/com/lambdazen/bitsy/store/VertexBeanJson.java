package com.lambdazen.bitsy.store;

import java.io.Serializable;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.ads.dict.DictionaryFactory;

@JsonPropertyOrder({"id", "v", "s", "p"})
public final class VertexBeanJson extends VertexBean implements Serializable {
    private static final long serialVersionUID = 8270238605124987367L;

    BitsyState state;

    @JsonCreator
    public VertexBeanJson(@JsonProperty("id") String uuidStr, 
    		@JsonProperty("l") String label,
            @JsonProperty("p") TreeMap<String, Object> properties, 
            @JsonProperty("v") int version, 
            @JsonProperty("s") BitsyState state) {
        super(UUID.fromString(uuidStr), label, DictionaryFactory.fromMap(properties), version);

        this.state = state;
    }

    public VertexBeanJson(UUID id, String label, Dictionary properties, int version, BitsyState state) {
        super(id, label, properties, version);

        this.state = state;
    }

    @JsonProperty("s")
    public BitsyState getState() {
        return state;
    }

    @JsonProperty("id")
    public String getIdStr() {
        return getId().uuidRepr();
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
}
