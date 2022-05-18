package com.lambdazen.bitsy.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyElement;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyGraph;
import com.lambdazen.bitsy.BitsyIsolationLevel;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.ICommitChanges;
import com.lambdazen.bitsy.ITransaction;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.index.IndexHelper;
import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.store.VertexBean;
import com.lambdazen.bitsy.util.EdgeIterator;
import com.lambdazen.bitsy.util.VertexIterator;

/** This class captures a transaction that is NOT thread-safe */
public class BitsyTransaction implements ITransaction, ICommitChanges {
    private static final Direction[] directions = new Direction[] {Direction.OUT, Direction.IN};

    private BitsyTransactionContext context;
    private BitsyIsolationLevel isolationLevel;
    private BitsyGraph graph;

    private boolean isOpen = false;

    public BitsyTransaction(BitsyTransactionContext context, BitsyIsolationLevel isolationLevel, BitsyGraph graph) {
        this.context = context;
        this.isolationLevel = isolationLevel;
        this.graph = graph;
    }

    @Override
    public Graph createThreadedTx() {
        // TP3 behavior of threaded graphs are different than TP2
        throw new UnsupportedOperationException("Graph does not support threaded transactions");

        // TODO: Reintroduce threaded transactions
        // return new ThreadedBitsyGraph(graph);
    }

    public BitsyGraph graph() {
        return graph;
    }

    @Override
    public void open() {
        // Transactions are automatically opened
        if (isOpen) {
            throw new IllegalStateException("The open() call was made multiple times to the same transaction");
        } else {
            this.isOpen = true;
        }
    }

    //@Override
    //public <T extends TraversalSource> T begin(final Class<T> traversalSourceClass) {
    //    return graph.traversal(traversalSourceClass);
    //}

    @Override
    public void commit() {
        this.save(true);
    }

    @Override
    public void rollback() {
        this.save(false);
    }

    @Override
    public void close() {
        // NEW TP3 behavior
        context.getCloseConsumer().accept(this);
    }

    public boolean isOpen() {
        return isOpen;
    }

    public boolean isStopped() {
        return !isOpen();
    }

    @Override
    public BitsyIsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    @Override
    public void setIsolationLevel(BitsyIsolationLevel level) {
        this.isolationLevel = level;
    }

    public void save(boolean commit) {
        try {
            if ((!isOpen) && (context.getReadWriteConsumer() == READ_WRITE_BEHAVIOR.MANUAL)) {
                throw new IllegalStateException("Commit/rollback called on a transaction that is not open");
            } else if (!commit) {
                // Nothing to do for rollback
                context.announceRollback(this);
            } else {
                // Commit the changes
                context.store.commit(this);
                context.announceCommit(this);
            }
        } finally {
            // Success or failure -- everything must go

            // Decouple from the context
            context.clear();

            // Close transaction
            isOpen = false;
        }
    }

    public void checkIfActive() throws BitsyException {
        readWrite();
    }

    public void validateForQuery(BitsyElement bitsyElement) throws BitsyException {
        // An element tied to a stopped transaction should not be queried
        checkIfActive();

        if (isDeleted(bitsyElement)) {
            throw new BitsyException(BitsyErrorCodes.ELEMENT_ALREADY_DELETED);
        }
    }

    private boolean isDeleted(BitsyElement bitsyElement) {
        if (bitsyElement.getState() == BitsyState.D) {
            return true;
        }

        if (bitsyElement instanceof BitsyEdge) {
            BitsyEdge edge = (BitsyEdge)bitsyElement;
            return (isDeletedVertex(edge.getInVertexId()) || isDeletedVertex(edge.getOutVertexId()));
        } else {
            return false;
        }
    }

    public Vertex getVertex(UUID id) throws BitsyException {
        // Only work on live transactions
        checkIfActive();

        // 1. Check if this vertex is defined in this transaction
        BitsyVertex ans = context.changedVertices.get(id);
        if (ans == null) {
            ans = context.unmodifiedVertices.get(id);
        }

        if (ans != null) {
            // A deleted vertex must not be returned
            if (isDeleted(ans)) {
                return null;
            } else {
                return ans;
            }
        }

        // 2. Get an unmodified vertex tied to this transaction
        ans = context.store.getBitsyVertex(this, id);
        if (ans != null) {
            // 3. Keep a reference if the isolation level is repeatable read
            if (isolationLevel == BitsyIsolationLevel.REPEATABLE_READ) {
                context.unmodifiedVertices.put((UUID)(ans.id()), ans);
            }
        }

        return ans;
    }

    public Edge getEdge(UUID id) throws BitsyException {
        // Only work on live transactions
        checkIfActive();

        // 1. Check if this edge is defined in this transaction
        BitsyEdge ans = context.changedEdges.get(id);
        if (ans == null) {
            ans = context.unmodifiedEdges.get(id);
        }

        if (ans != null) {
            // A deleted element must not be returned
            if (isDeleted(ans)) {
                return null;
            } else {
                return ans;
            }
        }

        // 2. Get the edge from the store
        ans = context.store.getBitsyEdge(this, id);
        if (ans == null) {
            // Not found.
            return null;
        }

        // 3. Make sure that both ends of this edge haven't been deleted
        if (isDeletedVertex(ans.getInVertexId()) || isDeletedVertex(ans.getOutVertexId())) {
            return null;
        }

        // 4. Keep a reference when the isolation level is repeatable read
        if (isolationLevel == BitsyIsolationLevel.REPEATABLE_READ) {
            context.unmodifiedEdges.put((UUID)(ans.id()), ans);
        }

        return ans;
    }

    private boolean isDeletedVertex(UUID id) {
        BitsyVertex changedVertex = context.changedVertices.get(id);

        return (changedVertex != null) && (changedVertex.getState() == BitsyState.D);
    }

    public Iterable<Edge> getEdges(BitsyVertex bitsyVertex, Direction dir, String... edgeLabels) throws BitsyException {
        // Only work on live transactions, where the vertex is valid (i.e., not deleted)
        validateForQuery(bitsyVertex);

        // Filter out the ones that are bad
        final List<Edge> mergedEdges = new ArrayList<Edge>();

        for (Direction myDir : directions) {
            if ((myDir == dir) || (dir == Direction.BOTH)) {
                //log.debug("Getting edges for dir {} and labels {}", myDir, Arrays.asList(edgeLabels));
                List<UUID> txEdgeIds = context.adjMap.getEdges((UUID)bitsyVertex.id(), myDir, edgeLabels);
                // Go over each edge in storeEdges and merge it with the changedEdges to get mergedEdges
                for (UUID edgeId : txEdgeIds) {
                    Edge edge = getEdge(edgeId);

                    // An end-point vertex may be deleted in this Tx, so this check is required
                    if (edge != null) {
                        mergedEdges.add(edge);
                        //log.debug("Merged edges.1 += {}", edge);
                    }
                }

                // Get the edges from the store
                // TODO: See if this can be made into a lazy data-structure
                List<EdgeBean> storeEdges = context.store.getEdges((UUID)bitsyVertex.id(), myDir, edgeLabels);

                // Go over each edge in storeEdges and merge it with the changedEdges to get mergedEdges
                for (EdgeBean edge : storeEdges) {
                    // Check if the edge has been changed
                    BitsyEdge changedEdge = context.changedEdges.get((UUID)edge.getId());
                    if (changedEdge == null) {
                        changedEdge = context.unmodifiedEdges.get((UUID)edge.getId());
                    }

                    if (changedEdge != null) {
                        // A Tx-specific version exists
                        if (changedEdge.getState() == BitsyState.D) {
                            // The edge has been deleted in this transaction. Skip this one
                        } else {
                            // Make sure that the edge isn't deleted
                            if (!isDeletedVertex(changedEdge.getVertexId(myDir.opposite()))) {
                                // Keep this edge, but give the version from this transaction
                                mergedEdges.add(changedEdge);

                                //log.debug("Merged edges.2 += {}", changedEdge);
                            }
                        }
                    } else {
                        // This edge hasn't been changed in this Tx, but the vertex might be deleted in this Tx
                        changedEdge = new BitsyEdge(edge, this, BitsyState.U);

                        // Make sure that the edge isn't deleted
                        if (isDeletedVertex(changedEdge.getVertexId(myDir.opposite()))) {
                            // The other vertex has been deleted by this transaction. Skip this one.
                        } else {
                            // Keep this edge as provided by the graph store (i.e., outside tx context)
                            mergedEdges.add(changedEdge);

                            //log.debug("Merged edges.3 += {}", changedEdge);

                            // Add it to the Tx context
                            context.unmodifiedEdges.put(edge.getId(), changedEdge);
                        }
                    }
                }
            }
        }

        return mergedEdges;
    }

    public void markForPropertyUpdate(BitsyElement bitsyElement) throws BitsyException {
        // An update is valid only if the element can be queried
        validateForQuery(bitsyElement);

        // If so, the element must be marked as modified
        bitsyElement.setState(BitsyState.M);

        // ...and must be added to the changedVertices if missing
        UUID id = (UUID)bitsyElement.id();
        if (bitsyElement instanceof BitsyVertex) {
            context.unmodifiedVertices.remove(id);
            context.changedVertices.put(id, (BitsyVertex)bitsyElement);
        } else {
            context.unmodifiedEdges.remove(id);

            BitsyEdge edge = (BitsyEdge)bitsyElement;
            context.changedEdges.put(id, edge);
        }
    }

    public void addVertex(BitsyVertex vertex) throws BitsyException {
        // Only work on live transactions
        checkIfActive();

        // If so, the vertex must be added to changedVertices
        UUID id = (UUID)vertex.id();
        context.changedVertices.put(id, vertex);
    }

    public void removeVertex(BitsyVertex vertex) throws BitsyException {
        // Only work on live transactions and valid vertices
        validateForQuery(vertex);

        // Ensure that the edge was created in this transaction
        if (vertex.getTransaction() != this) {
            throw new BitsyException(BitsyErrorCodes.REMOVING_VERTEX_FROM_ANOTHER_TX, "Vertex " + vertex.id() + " belongs to a different transaction");
        }

        // The element must be marked as deleted
        vertex.setState(BitsyState.D);

        // Add to changed vertices, if not already available
        UUID id = (UUID)vertex.id();
        context.changedVertices.put(id, vertex);
        context.unmodifiedVertices.remove(id);

        // All edges related to this vertex, must be marked as deleted
        context.adjMap.removeVertex(id);
    }

    public void addEdge(BitsyEdge edge) throws BitsyException {
        // Only work on live transactions and valid edges
        validateForQuery(edge);

        // Ensure that both end-points of the edge have not been deleted in this Tx
        if (isDeletedVertex(edge.getInVertexId())) {
            throw new BitsyException(BitsyErrorCodes.ADDING_EDGE_TO_A_DELETED_VERTEX);
        }

        if (isDeletedVertex(edge.getOutVertexId())) {
            throw new BitsyException(BitsyErrorCodes.ADDING_EDGE_FROM_A_DELETED_VERTEX);
        }

        UUID id = (UUID)edge.id();
        context.changedEdges.put(id, edge);

        // and the adjacency map
        context.adjMap.addEdge(id, edge.getOutVertexId(), edge.label(), edge.getInVertexId(), edge.getVersion());
    }



    public void removeEdge(BitsyEdge edge) throws BitsyException {
        // Only work on live transactions. It is OK if the vertex is already modified/deleted.
        checkIfActive();

        // Ensure that the edge was created in this transaction
        if (edge.getTransaction() != this) {
            throw new BitsyException(BitsyErrorCodes.REMOVING_EDGE_FROM_ANOTHER_TX, "Edge " + edge.id() + " belongs to a different transaction");
        }

        // The element must be marked as deleted
        edge.setState(BitsyState.D);

        // Add to changed edges, if not already available
        UUID id = (UUID)edge.id();
        context.changedEdges.put(id, edge);
        context.unmodifiedEdges.remove(id);

        // Remove from adjacency map
        context.adjMap.removeEdge((UUID)edge.id(), edge.getOutVertexId(), edge.label(), edge.getInVertexId());
    }

    public Collection<BitsyVertex> getVertexChanges() {
        return context.changedVertices.values();
    }

    public Collection<BitsyEdge> getEdgeChanges() {
        return context.changedEdges.values();
    }

    @Override
    public Iterator<Vertex> getAllVertices() {
        // Only work on live transactions
        checkIfActive();

        // 1. Get a concurrently navigable list of vertices
        Collection<VertexBean> allVertices = context.store.getAllVertices();

        // 2. Wrap it around an iterator for this transaction. Idea is for the
        // transaction to take priority over vertices in the store. Creating a
        // copy to avoid concurrent modification exceptions
        return (Iterator)new VertexIterator(this, new ArrayList<BitsyVertex>(getVertexChanges()), allVertices);
    }

    @Override
    public Iterator<Edge> getAllEdges() {
        // Only work on live transactions
        checkIfActive();

        // 1. Get a concurrently navigable list of vertices
        Collection<EdgeBean> allEdges = context.store.getAllEdges();

        // 2. Wrap it around an iterator for this transaction. Idea is for the
        // transaction to take priority over vertices in the store. Creating a
        // copy to avoid concurrent modification exceptions
        return (Iterator)new EdgeIterator(this, new ArrayList<BitsyEdge>(getEdgeChanges()), allEdges);
    }

    @Override
    public Iterator<BitsyVertex> lookupVertices(String key, Object value) {
        // Only work on live transactions
        checkIfActive();

        // 1. Get the list of vertices that match the given key value form this Tx
        Collection<VertexBean> vertices;
        try {
            vertices = context.store.lookupVertices(key, value);
        } catch (BitsyException e) {
            if ((e.getErrorCode() == BitsyErrorCodes.MISSING_INDEX) && context.store.allowFullGraphScans()) {
                vertices = IndexHelper.filterVertexBeansByKeyValue(context.store.getAllVertices(), key, value);
            } else {
                throw e;
            }
        }

        // 2. Get the matching vertices in this transaction
        Collection<BitsyVertex> vertexChanges;
        if (getIsolationLevel() == BitsyIsolationLevel.READ_COMMITTED) {
            vertexChanges = getVertexChanges();
        } else {
            vertexChanges = new ArrayList<BitsyVertex>();
            vertexChanges.addAll(getVertexChanges());
            vertexChanges.addAll(context.unmodifiedVertices.values());
        }

        Collection<BitsyVertex> txVertices = IndexHelper.filterElementsByKeyValue(vertexChanges, key, value);

        // 3. Wrap it around an iterator for this transaction. Idea is for the
        // transaction to take priority over vertices in the store.
        return new VertexIterator(this, txVertices, vertices, vertexChanges);
    }

    @Override
    public Iterator<BitsyEdge> lookupEdges(String key, Object value) {
        // Only work on live transactions
        checkIfActive();

        // 1. Get the list of vertices that match the given key value form this Tx
        Collection<EdgeBean> edges;
        try {
            edges = context.store.lookupEdges(key, value);
        } catch (BitsyException e) {
            if ((e.getErrorCode() == BitsyErrorCodes.MISSING_INDEX) && context.store.allowFullGraphScans()) {
                edges = IndexHelper.filterEdgeBeansByKeyValue(context.store.getAllEdges(), key, value);
            } else {
                throw e;
            }
        }

        // 2. Get the matching vertices in this transaction
        Collection<BitsyEdge> edgeChanges;
        if (getIsolationLevel() == BitsyIsolationLevel.READ_COMMITTED) {
            edgeChanges = getEdgeChanges();
        } else {
            edgeChanges = new ArrayList<BitsyEdge>();
            edgeChanges.addAll(getEdgeChanges());
            edgeChanges.addAll(context.unmodifiedEdges.values());
        }

        Collection<BitsyEdge> txEdges = IndexHelper.filterElementsByKeyValue(edgeChanges, key, value);

        // 3. Wrap it around an iterator for this transaction. Idea is for the
        // transaction to take priority over vertices in the store.
        return new EdgeIterator(this, txEdges, edges, edgeChanges);
    }

    // Added for Tinkerpop 3
    @Override
    public void readWrite() {
        context.getReadWriteConsumer().accept(this);
    }

    @Override
    public Transaction onReadWrite(Consumer<Transaction> consumer) {
        context.onReadWrite(consumer);
        return this;
    }

    @Override
    public Transaction onClose(Consumer<Transaction> consumer) {
        context.onClose(consumer);
        return this;
    }

    @Override
    public void addTransactionListener(Consumer<Status> listener) {
        context.addTransactionListener(listener);
    }

    @Override
    public void removeTransactionListener(Consumer<Status> listener) {
        context.removeTransactionListener(listener);
    }

    @Override
    public void clearTransactionListeners() {
        context.clearTransactionListeners();
    }

    // Added for Tinkerpop 3.5
    public <T extends TraversalSource> T begin(final Class<T> traversalSourceClass) {
        throw new UnsupportedOperationException("Bitsy does not support begin(). Please use open, commit, rollback and close");
    }

}
