/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.lambdazen.bitsy.structure;

import com.lambdazen.bitsy.BitsyGraph;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.EdgeTest;
import org.apache.tinkerpop.gremlin.structure.VertexTest;
import org.junit.runner.RunWith;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(BitsyGraphStructureTestSuite.class)
@GraphProviderClass(provider = BitsyTestGraphProvider.class, graph = BitsyGraph.class)
public class BitsyGraphStructureTestSuite extends AbstractGremlinSuite {
    /*
     * This list of tests in the suite that will be executed.  Gremlin developers should add to this list
     * as needed to enforce tests upon implementations.
     */
    private static final Class<?>[] allTests = new Class<?>[] {
        VertexTest.class, EdgeTest.class,
        //    	VertexPropertyTest.class,
        //    	GraphTest.class,
        //    	FeatureSupportTest.class,
        //    	PropertyTest.class,
        //    	VariablesTest.class,

        //    	DetachedGraphTest.class,
        //      DetachedVertexPropertyTest.class,
        //        DetachedPropertyTest.class
        //    	DetachedVertexTest.class,
        //        DetachedEdgeTest.class,

        //        ReferenceGraphTest.class,
        //        ReferenceEdgeTest.class,
        //        ReferenceVertexPropertyTest.class,
        //        ReferenceVertexTest.class,

        //        CommunityGeneratorTest.class,
        //        DistributionGeneratorTest.class,
        //        GraphConstructionTest.class,

        //        TransactionTest.class,

        /* SERIALIZATION TESTS WON'T WORK --
        IoPropertyTest.class,
        IoTest.class,
        IoVertexTest.class,
        IoCustomTest.class,
        IoEdgeTest.class,
        IoGraphTest.class,
        SerializationTest.class,
        StarGraphTest.class // has failure related to UUID and kryo
        */

    };

    public BitsyGraphStructureTestSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, null, false, TraversalEngine.Type.STANDARD);
    }
}
