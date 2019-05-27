package com.lambdazen.bitsy.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.ads.dict.DictionaryFactory;
import com.lambdazen.bitsy.store.Record.RecordType;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

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



    public void testDictionaryMaxProperties() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectReader reader = mapper.reader(VertexBeanJson.class);

        String json = "{\"id\":\"40026fee-498a-4cd9-afad-1ae31363c13e\",\"v\":1,\"s\":\"M\",\"p\":{\"a00\":\"00\",\"a01\":\"01\",\"a02\":\"02\",\"a03\":\"03\",\"a04\":\"04\",\"a05\":\"05\",\"a06\":\"06\",\"a07\":\"07\",\"a08\":\"08\",\"a09\":\"09\",\"a10\":\"10\",\"a11\":\"11\",\"a12\":\"12\",\"a13\":\"13\",\"a14\":\"14\",\"a15\":\"15\",\"a16\":\"16\",\"a17\":\"17\",\"a18\":\"18\",\"a19\":\"19\",\"a20\":\"20\",\"a21\":\"21\",\"a22\":\"22\",\"a23\":\"23\",\"a24\":\"24\",\"a25\":\"25\",\"a26\":\"26\",\"a27\":\"27\",\"a28\":\"28\",\"a29\":\"29\",\"a30\":\"30\",\"a31\":\"31\",\"a32\":\"32\",\"a33\":\"33\",\"a34\":\"34\",\"a35\":\"35\",\"a36\":\"36\",\"a37\":\"37\",\"a38\":\"38\",\"a39\":\"39\",\"a40\":\"40\",\"a41\":\"41\",\"a42\":\"42\",\"a43\":\"43\",\"a44\":\"44\",\"a45\":\"45\",\"a46\":\"46\",\"a47\":\"47\",\"a48\":\"48\",\"a49\":\"49\",\"a50\":\"50\"}}";

        VertexBeanJson vBean = reader.readValue(json);

        int count = vBean.getProperties().size();
        assertEquals(51, count);
    }
}
