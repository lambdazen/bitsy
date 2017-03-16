package com.lambdazen.bitsy.gremlin;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.wrapper.BitsyAutoReloadingGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

// Bitsy graph step based Tinkerpop's Neo4j implementation
public final class BitsyGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public BitsyGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

	private BitsyGraph getBitsyGraph() {
		Graph baseGraph = this.getTraversal().getGraph().get();
        final BitsyGraph graph;
		if (baseGraph instanceof BitsyAutoReloadingGraph) {
			return ((BitsyAutoReloadingGraph)baseGraph).getBaseGraph();
		} else {
			return (BitsyGraph) baseGraph;
		}
	}

    private Iterator<? extends Vertex> vertices() {
        return lookupVertices(getBitsyGraph(), this.hasContainers, this.ids);
    }


	private Iterator<Vertex> lookupVertices(final BitsyGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
		// ids are present, filter on them first
		if (ids.length > 0)
			return IteratorUtils.filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));

		// get a label being search on
		Optional<String> label = hasContainers.stream()
				.filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
				.filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
				.map(hasContainer -> (String) hasContainer.getValue())
				.findAny();
		if (!label.isPresent())
			label = hasContainers.stream()
			.filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
			.map(hasContainer -> (String) hasContainer.getValue())
			.findAny();

		// Labels aren't indexed in Bitsy, only keys -- so do a full scan
		for (final HasContainer hasContainer : hasContainers) {
			if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey().equals(T.label.getAccessor())) {
				if (graph.getIndexedKeys(Vertex.class).contains(hasContainer.getKey())) {
					// Find a vertex by key/value 
					return IteratorUtils.stream(graph.verticesByIndex(hasContainer.getKey(), hasContainer.getValue()))
							.map(vertex -> (Vertex) vertex)
							.filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
				}
			}
		}
		return IteratorUtils.filter(graph.vertices(), vertex -> HasContainer.testAll(vertex, hasContainers));
	}

    private Iterator<? extends Edge> edges() {
        //return IteratorUtils.filter(this.getTraversal().getGraph().get().edges(this.ids), edge -> HasContainer.testAll(edge, this.hasContainers));
        return lookupEdges(getBitsyGraph(), this.hasContainers, this.ids);
    }

	private Iterator<Edge> lookupEdges(final BitsyGraph graph, final List<HasContainer> hasContainers, final Object... ids) {
		// ids are present, filter on them first
		if (ids.length > 0)
			return IteratorUtils.filter(graph.edges(ids), vertex -> HasContainer.testAll(vertex, hasContainers));

		// get a label being search on
		Optional<String> label = hasContainers.stream()
				.filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
				.filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
				.map(hasContainer -> (String) hasContainer.getValue())
				.findAny();
		if (!label.isPresent())
			label = hasContainers.stream()
			.filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
			.map(hasContainer -> (String) hasContainer.getValue())
			.findAny();

		// Labels aren't indexed in Bitsy, only keys -- so do a full scan
		for (final HasContainer hasContainer : hasContainers) {
			if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey().equals(T.label.getAccessor())) {
				if (graph.getIndexedKeys(Vertex.class).contains(hasContainer.getKey())) {
					// Find a vertex by key/value 
					return IteratorUtils.stream(graph.edgesByIndex(hasContainer.getKey(), hasContainer.getValue()))
							.map(edge -> (Edge) edge)
							.filter(edge -> HasContainer.testAll(edge, hasContainers)).iterator();
				}
			}
		}
		return IteratorUtils.filter(graph.edges(), vertex -> HasContainer.testAll(vertex, hasContainers));
	}

	@Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return 0 == this.ids.length ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
