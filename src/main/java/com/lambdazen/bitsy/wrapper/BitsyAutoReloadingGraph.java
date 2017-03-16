package com.lambdazen.bitsy.wrapper;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.BitsyVertex;

public class BitsyAutoReloadingGraph implements Graph {
    private BitsyGraph graph;

    public BitsyAutoReloadingGraph(BitsyGraph g) {
        this.graph = g;
    }
    
    public BitsyGraph getBaseGraph() {
        return graph;
    }

    public String toString() {
        return "bitsyautoreloadinggraph[" + getBaseGraph().toString() + "]";
    }

    public static final BitsyAutoReloadingGraph open(Configuration configuration) {
    	return new BitsyAutoReloadingGraph(BitsyGraph.open(configuration));
    }

    @Override
	public Vertex addVertex(Object... keyValues) {
        BitsyVertex base = (BitsyVertex)(graph.addVertex(keyValues));
        
        return new BitsyAutoReloadingVertex(graph, base);
	}

	@Override
	public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
		return (C)graph.compute(graphComputerClass);
	}

	@Override
	public GraphComputer compute() throws IllegalArgumentException {
		return graph.compute();
	}

	@Override
	public Iterator<Vertex> vertices(Object... vertexIds) {
		Iterator<Vertex> result = graph.vertices(vertexIds);
		return new VertexIterator(graph, result);
	}

	@Override
	public Iterator<Edge> edges(Object... edgeIds) {
		Iterator<Edge> result = graph.edges(edgeIds);

		return new EdgeIterator(graph, result);
	}

	@Override
	public Transaction tx() {
		return graph.tx();
	}

	@Override
	public void close() throws Exception {
		graph.close();		
	}

	@Override
	public Variables variables() {
		return graph.variables();
	}

	@Override
	public Configuration configuration() {
		return graph.configuration();
	}

    @Override
    public Graph.Features features() {
        return graph.features();
    }

    public static class VertexIterator implements Iterator<Vertex> {
        BitsyGraph graph;
        Iterator<Vertex> iter;

        public VertexIterator(BitsyGraph g, Iterator<Vertex> iter) {
            this.graph = g;
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Vertex next() {
            return new BitsyAutoReloadingVertex(graph, (BitsyVertex)(iter.next()));
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }

    public static class EdgeIterator implements Iterator<Edge> {
        BitsyGraph graph;
        Iterator<Edge> iter;

        public EdgeIterator(BitsyGraph g, Iterator<Edge> iter) {
            this.graph = g;
            this.iter = iter;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Edge next() {
            return new BitsyAutoReloadingEdge(graph, (BitsyEdge)(iter.next()));
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }
}
