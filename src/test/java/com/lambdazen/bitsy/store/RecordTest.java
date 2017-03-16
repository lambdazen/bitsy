package com.lambdazen.bitsy.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.DictionaryFactory;
import com.lambdazen.bitsy.store.Record.RecordType;

import junit.framework.TestCase;

public class RecordTest extends TestCase {
    public RecordTest() {
    }
    
    public void testRecord() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TreeMap<String, Object> props = new TreeMap<String, Object>();
        
        UUID edgeId = UUID.fromString("9d09e705-fac4-409b-a0bb-74883fa21313");
        UUID outVId = UUID.fromString("25f1b840-c521-4398-ac84-9d1a0e305f4c");
        UUID inVId = UUID.fromString("2c390534-5f50-4924-8792-a06293db4241");

        VertexBean vBean = new VertexBeanJson(outVId, null, null, 1, BitsyState.D);
        props.put("test \"with\" quotes", null);
        List<Object> bar = new ArrayList<Object>();
        bar.add("another''\"one\"");
        bar.add("colon:{}+!#&*(!@#=");
        props.put("foo", bar);
        String str = Record.generateDBLine(RecordType.V, mapper.writeValueAsString(vBean));
        assertEquals("V={\"id\":\"25f1b840-c521-4398-ac84-9d1a0e305f4c\",\"v\":1,\"s\":\"D\",\"p\":null,\"l\":null}#42fe79ab\n",
        		str);

        VertexBean vBean2 = new VertexBeanJson(inVId, "foo", null, 1, BitsyState.D);
        props.put("test \"with\" quotes", null);
        props.put("foo", bar);
        str = Record.generateDBLine(RecordType.V, mapper.writeValueAsString(vBean2));
        assertEquals("V={\"id\":\"2c390534-5f50-4924-8792-a06293db4241\",\"v\":1,\"s\":\"D\",\"p\":null,\"l\":\"foo\"}#3656bf28\n",
        		str);
        
        EdgeBean edgeBean = new EdgeBean(edgeId, DictionaryFactory.fromMap(props), Integer.MIN_VALUE, null, vBean, vBean2);
        
        EdgeBean edgeBeanJson = new EdgeBeanJson("" + edgeId, props, Integer.MIN_VALUE, null, vBean.getIdStr(), vBean2.getIdStr(), BitsyState.M);
        str = Record.generateDBLine(RecordType.E, mapper.writeValueAsString(edgeBeanJson));
        
        System.out.println("str: " + str.replaceAll("[\\\\]", "\\\\\\\\").replaceAll("[\"]", "\\\\\""));
        assertEquals("E={\"id\":\"9d09e705-fac4-409b-a0bb-74883fa21313\",\"v\":-2147483648,\"s\":\"M\",\"o\":\"25f1b840-c521-4398-ac84-9d1a0e305f4c\",\"l\":null,\"i\":\"2c390534-5f50-4924-8792-a06293db4241\",\"p\":{\"foo\":[\"another''\\\"one\\\"\",\"colon:{}+!#&*(!@#=\"],\"test \\\"with\\\" quotes\":null}}#4cf814b7\n",
                str);
        //assertEquals("E={\"class\":\"com.lambdazen.bitsy.store.EdgeBean\",\"id\":\"9d09e705-fac4-409b-a0bb-74883fa21313\",\"v\":-2147483648,\"s\":\"M\",\"o\":\"25f1b840-c521-4398-ac84-9d1a0e305f4c\",\"l\":null,\"i\":\"2c390534-5f50-4924-8792-a06293db4241\",\"p\":{\"foo\":[\"another''\\\"one\\\"\",\"colon:{}+!#&*(!@#=\"],\"test \\\"with\\\" quotes\":null}}#98e95483",
        //        str);
        
        str = str.trim();
        Record edgeR = Record.parseRecord(str, 12, null);
        assertEquals(RecordType.E, edgeR.getType());
        assertEquals(str.substring(2, str.lastIndexOf("#")), edgeR.getJson());
        
        try {
            // Wrong hashcode
            Record.parseRecord(str.substring(0, str.length()-1), 13, null);
            fail("wrong hashcode");
        } catch (BitsyException e) {
            // all ok
        }
        
        EdgeBeanJson edgeBean2 = (EdgeBeanJson)mapper.readValue(edgeR.getJson(), EdgeBeanJson.class);
        assertEquals(edgeBean.getId(), edgeBean2.getId());
        assertEquals(edgeBean.getLabel(), edgeBean2.getLabel());
        assertEquals(edgeBean.getVersion(), edgeBean2.getVersion());
        assertEquals(edgeBean.getInVertexId(), edgeBean2.getInVertexId());
        assertEquals(edgeBean.getOutVertexId(), edgeBean2.getOutVertexId());
        assertEquals(edgeBean.getProperties(), edgeBean2.getProperties());
        assertEquals(edgeBean.getProperties(), edgeBean2.getProperties());
        assertEquals(BitsyState.M, edgeBean2.getState());
    }
}
