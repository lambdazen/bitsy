package com.lambdazen.bitsy.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

// Bitsy traversal strategy based Tinkerpop's Neo4j implementation
public class BitsyTraversalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final long serialVersionUID = 6405194525894707932L;
    private static final BitsyTraversalStrategy INSTANCE = new BitsyTraversalStrategy();

    private BitsyTraversalStrategy() {}

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            final BitsyGraphStep<?, ?> bitsyGraphStep = new BitsyGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, bitsyGraphStep, traversal);
            Step<?, ?> currentStep = bitsyGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(bitsyGraphStep, hasContainer))
                            bitsyGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
        }
    }

    public static BitsyTraversalStrategy instance() {
        return INSTANCE;
    }
}
