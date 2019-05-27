// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.lambdazen.bitsy;

import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.store.VertexBean;
import com.lambdazen.bitsy.store.VertexBeanJson;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.*;
import org.apache.tinkerpop.shaded.jackson.core.type.WritableTypeId;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class BitsyGraphSONModule extends TinkerPopJacksonModule {

    private static final String TYPE_NAMESPACE = "bitsy";

    private static final Map<Class, String> TYPE_DEFINITIONS = Collections.unmodifiableMap(
            new LinkedHashMap<Class, String>() {{
                put(UUID.class, "UUID");
                put(VertexBean.class, "VertexBean");
                put(EdgeBean.class, "EdgeBean");
            }});

    private BitsyGraphSONModule() {
        super("bitsy");
        addSerializer(UUID.class, new UUIDSerializer());
        addDeserializer(UUID.class, new UUIDDeserializer());

//        addSerializer(VertexBean.class, new UUIDSerializer());
//        addSerializer(EdgeBean.class, new UUIDSerializer());
    }

    private static final BitsyGraphSONModule INSTANCE = new BitsyGraphSONModule();

    public static final BitsyGraphSONModule getInstance() {
        return INSTANCE;
    }

    @Override
    public Map<Class, String> getTypeDefinitions() {
        return TYPE_DEFINITIONS;
    }

    @Override
    public String getTypeNamespace() {
        return TYPE_NAMESPACE;
    }

    public static class UUIDSerializer extends StdSerializer<UUID> {

        public UUIDSerializer() {
            super(UUID.class);
        }

        @Override
        public void serialize(final UUID uuid,
                              final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException
        {
            String uuidStr = UUID.toString(uuid);
            jsonGenerator.writeString(uuidStr);
        }

        @Override
        public void serializeWithType(final UUID uuid,
                                      final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider,
                                      final TypeSerializer typeSerializer)
                throws IOException, JsonProcessingException {
            // since jackson 2.9, must keep track of `typeIdDef` in order to close it properly
            final WritableTypeId typeIdDef = typeSerializer.writeTypePrefix(jsonGenerator, typeSerializer.typeId(uuid, JsonToken.VALUE_STRING));
            String uuidStr = UUID.toString(uuid);
            jsonGenerator.writeString(uuidStr);
            typeSerializer.writeTypeSuffix(jsonGenerator, typeIdDef);
        }
    }

    public static class UUIDDeserializer extends StdDeserializer<UUID> {
        public UUIDDeserializer() {
            super(UUID.class);
        }

        @Override
        public UUID deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            jsonParser.nextToken();
            final String uuidStr = deserializationContext.readValue(jsonParser, String.class);
            return UUID.fromString(uuidStr);
        }
    }

}
