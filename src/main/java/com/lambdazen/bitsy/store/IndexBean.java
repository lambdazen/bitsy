package com.lambdazen.bitsy.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

@JsonPropertyOrder({"type", "key"})
public class IndexBean {
    int type;
    String key;

    @JsonCreator
    public IndexBean(@JsonProperty("type") int type, @JsonProperty("key") String key) {
        this.type = type;
        this.key = key;
    }

    @JsonProperty("type")
    public int getType() {
        return type;
    }

    @JsonProperty("key")
    public String getKey() {
        return key;
    }

    @JsonIgnore
    public Class getIndexClass() {
        if (type == 0) {
            return Vertex.class;
        } else if (type == 1) {
            return Edge.class;
        } else {
            throw new BitsyException(BitsyErrorCodes.DATABASE_IS_CORRUPT, "Unrecognized index type " + type);
        }
    }
}
