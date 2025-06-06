package com.lambdazen.bitsy.structure;

import com.lambdazen.bitsy.BitsyGraph;
import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.BranchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.ChooseTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.OptionalTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AndTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CoinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.CyclicPathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.OrTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SimplePathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CoalesceTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ConstantTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LoopsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MinTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.UnfoldTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ValueMapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupCountTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StoreTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeTest;
import org.junit.runner.RunWith;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

@RunWith(BitsyProcessStandardTestSuite.class)
@GraphProviderClass(provider = BitsyTestGraphProvider.class, graph = BitsyGraph.class)
public class BitsyProcessStandardTestSuite extends AbstractGremlinSuite {
    /**
     * This list of tests in the suite that will be executed as part of this suite.
     */
    private static final Class<?>[] allTests = new Class<?>[] {

        // branch
        //            BranchTest.Traversals.class,
        //            ChooseTest.Traversals.class,
        //            OptionalTest.Traversals.class,
        //            LocalTest.Traversals.class,
        //            RepeatTest.Traversals.class,
        //            UnionTest.Traversals.class,

        // filter
        //            AndTest.Traversals.class,
        //            CoinTest.Traversals.class,
        //            CyclicPathTest.Traversals.class,
        //            DedupTest.Traversals.class,
        //            DropTest.Traversals.class,
        //            FilterTest.Traversals.class,
        HasTest.Traversals.class, IsTest.Traversals.class,
        //            OrTest.Traversals.class,
        //            RangeTest.Traversals.class,
        //            SampleTest.Traversals.class,
        //            SimplePathTest.Traversals.class,
        //            TailTest.Traversals.class,
        //            WhereTest.Traversals.class,

        // map
        //            AddEdgeTest.Traversals.class,
        //            AddVertexTest.Traversals.class,
        //            CoalesceTest.Traversals.class,
        //            ConstantTest.Traversals.class,
        //            CountTest.Traversals.class,
        //            FlatMapTest.Traversals.class,
        //            FoldTest.Traversals.class,
        //            GraphTest.Traversals.class,
        //            LoopsTest.Traversals.class,
        //            MapTest.Traversals.class,
        //            MapKeysTest.Traversals.class,
        //            MapValuesTest.Traversals.class,
        //            MatchTest.CountMatchTraversals.class,
        //            MatchTest.GreedyMatchTraversals.class,
        //            MaxTest.Traversals.class,
        //            MeanTest.Traversals.class,
        //            MinTest.Traversals.class,
        //            SumTest.Traversals.class,
        //            OrderTest.Traversals.class,
        //            PathTest.Traversals.class,
        //            ProfileTest.Traversals.class, // NOT SURE WHY THIS IS FAILING?!
        //            ProjectTest.Traversals.class,

        //            PropertiesTest.Traversals.class,
        //            SelectTest.Traversals.class,
        //            VertexTest.Traversals.class,
        //            UnfoldTest.Traversals.class,
        //            ValueMapTest.Traversals.class,

        // sideEffect
        //            AggregateTest.Traversals.class,
        //            ExplainTest.Traversals.class,
        //            GroupTest.Traversals.class,
        //            GroupTestV3d0.Traversals.class,
        //            GroupCountTest.Traversals.class,
        //            InjectTest.Traversals.class,
        //            SackTest.Traversals.class,
        //            SideEffectCapTest.Traversals.class,
        //            SideEffectTest.Traversals.class,
        //            StoreTest.Traversals.class,
        //            SubgraphTest.Traversals.class,
        //            TreeTest.Traversals.class,

        // compliance
        //            ComplexTest.Traversals.class,
        //            CoreTraversalTest.class,
        //            TraversalInterruptionTest.class,

        // creations
        //            TranslationStrategyProcessTest.class,

        // decorations
        //            ElementIdStrategyProcessTest.class,
        //            EventStrategyProcessTest.class,
        //            ReadOnlyStrategyProcessTest.class,
        //            PartitionStrategyProcessTest.class,
        //            SubgraphStrategyProcessTest.class
    };

    /**
     * A list of the minimum set of base tests that Gremlin flavors should implement to be compliant with Gremlin.
     */
    private static final Class<?>[] testsToEnforce = new Class<?>[] {
        // branch
        BranchTest.class,
        ChooseTest.class,
        OptionalTest.class,
        LocalTest.class,
        RepeatTest.class,
        UnionTest.class,

        // filter
        AndTest.class,
        CoinTest.class,
        CyclicPathTest.class,
        DedupTest.class,
        DropTest.class,
        FilterTest.class,
        HasTest.class,
        IsTest.class,
        OrTest.class,
        RangeTest.class,
        SampleTest.class,
        SimplePathTest.class,
        TailTest.class,
        WhereTest.class,

        // map
        AddEdgeTest.class,
        AddVertexTest.class,
        CoalesceTest.class,
        ConstantTest.class,
        CountTest.class,
        FlatMapTest.class,
        FoldTest.class,
        LoopsTest.class,
        MapTest.class,
        // MapKeysTest.class,
        // MapValuesTest.class,
        MatchTest.class,
        MaxTest.class,
        MeanTest.class,
        MinTest.class,
        SumTest.class,
        OrderTest.class,
        PathTest.class,
        PropertiesTest.class,
        ProfileTest.class,
        ProjectTest.class,
        SelectTest.class,
        VertexTest.class,
        UnfoldTest.class,
        ValueMapTest.class,

        // sideEffect
        AggregateTest.class,
        GroupTest.class,
        GroupCountTest.class,
        InjectTest.class,
        SackTest.class,
        SideEffectCapTest.class,
        SideEffectTest.class,
        StoreTest.class,
        SubgraphTest.class,
        TreeTest.class,
    };

    /**
     * This constructor is used by JUnit and will run this suite with its concrete implementations of the
     * {@code testsToEnforce}.
     */
    public BitsyProcessStandardTestSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, testsToEnforce, false, TraversalEngine.Type.STANDARD);
    }
}
