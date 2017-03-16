package com.lambdazen.bitsy.store;

import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.ads.dict.DictionaryFactory;

public class EdgeBeanJson extends EdgeBean {
    private static final long serialVersionUID = 3554788509798848887L;

    private UUID outVertexId, inVertexId;
    private BitsyState state;

    @JsonCreator
    public EdgeBeanJson(@JsonProperty("id") String idStr, 
            @JsonProperty("p") TreeMap<String, Object> properties, 
            @JsonProperty("v") int version, 
            @JsonProperty("l") String label, 
            @JsonProperty("o") String outVertexIdStr, 
            @JsonProperty("i") String inVertexIdStr, 
            @JsonProperty("s") BitsyState state) {
        this(UUID.fromString(idStr), DictionaryFactory.fromMap(properties), version, 
                label, UUID.fromString(outVertexIdStr), UUID.fromString(inVertexIdStr), 
                state);
    }
    
    public EdgeBeanJson(UUID id, Dictionary properties, int version, 
            String label, UUID outVertexId, UUID inVertexId, 
            BitsyState state) {
        super(id, properties, version, label, null, null);

        this.outVertexId = outVertexId;
        this.inVertexId = inVertexId;
        this.state = state;
    }

    @JsonIgnore
    @Override
    public UUID getInVertexId() {
        return inVertexId;
    }
    
    @JsonIgnore
    @Override
    public UUID getOutVertexId() {
        return outVertexId;
    }

    @JsonProperty("i")
    public String getInVertexIdStr() {
        return UUID.toString(inVertexId);
    }
    
    @JsonProperty("o")
    public String getOutVertexIdStr() {
        return UUID.toString(outVertexId);
    }

    @JsonProperty("s")
    public BitsyState getState() {
        return state;
    }
}
