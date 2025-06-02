package com.lambdazen.bitsy;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BitsyFeatures implements Features {
    private final GraphFeatures graphFeatures = new BitsyGraphFeatures();

    private final VertexFeatures vertexFeatures = new BitsyVertexFeatures();

    private final EdgeFeatures edgeFeatures = new BitsyEdgeFeatures();

    private boolean isPersistent;

    public BitsyFeatures(boolean isPersistent) {
        this.isPersistent = isPersistent;
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class BitsyGraphFeatures implements Features.GraphFeatures {
        private final Features.VariableFeatures variablesFeatures = new BitsyVariableFeatures();

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public Features.VariableFeatures variables() {
            return variablesFeatures;
        }

        @Override
        public boolean supportsPersistence() {
            return isPersistent;
        }

        // Yes for transactions

        // TODO: Change from no for threaded transactions to yes -- semantics seems to have changed per
        // shouldNotReuseThreadedTransaction
        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public class BitsyVariableFeatures implements Features.VariableFeatures {
        @Override
        public boolean supportsVariables() {
            return false;
        }

        @Override
        public boolean supportsBooleanValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleValues() {
            return false;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return false;
        }

        @Override
        public boolean supportsLongValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsByteValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }
    }

    public class BitsyVertexFeatures implements Features.VertexFeatures {
        private final Features.VertexPropertyFeatures vertexPropertyFeatures = new BitsyGraphPropertyFeatures();

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMetaProperties() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        // Yes for add and remove vertices
    }

    public class BitsyEdgeFeatures implements Features.EdgeFeatures {
        private final Features.EdgePropertyFeatures edgePropertyFeatures = new BitsyGraphPropertyFeatures();

        @Override
        public Features.EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        // Yes for add and remove edges
    }

    public class BitsyGraphPropertyFeatures implements Features.VertexPropertyFeatures, Features.EdgePropertyFeatures {
        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        // Yes for all property types
    }
}
