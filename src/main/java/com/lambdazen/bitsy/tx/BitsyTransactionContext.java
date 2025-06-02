package com.lambdazen.bitsy.tx;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.IEdge;
import com.lambdazen.bitsy.IGraphStore;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.store.AdjacencyMap;
import com.lambdazen.bitsy.store.IEdgeRemover;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR;
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR;
import org.apache.tinkerpop.gremlin.structure.Transaction.Status;

public class BitsyTransactionContext {
    Map<UUID, BitsyVertex> unmodifiedVertices;
    Map<UUID, BitsyEdge> unmodifiedEdges;
    Map<UUID, BitsyVertex> changedVertices;
    Map<UUID, BitsyEdge> changedEdges;
    IGraphStore store;
    AdjacencyMap adjMap;
    List<Consumer<Transaction.Status>> transactionListeners;

    Consumer<Transaction> readWriteConsumer = READ_WRITE_BEHAVIOR.AUTO;

    // The default close behavior in TP3 changed to rollback from commit in TP2
    Consumer<Transaction> closeConsumer = CLOSE_BEHAVIOR.ROLLBACK;

    public BitsyTransactionContext(IGraphStore store) {
        this.unmodifiedVertices = new HashMap<UUID, BitsyVertex>();
        this.unmodifiedEdges = new HashMap<UUID, BitsyEdge>();
        this.changedVertices = new HashMap<UUID, BitsyVertex>();
        this.changedEdges = new HashMap<UUID, BitsyEdge>();
        this.store = store;
        this.transactionListeners = new ArrayList<Consumer<Transaction.Status>>();

        this.adjMap = new AdjacencyMap(false, new IEdgeRemover() {
            @Override
            public IEdge removeEdge(UUID id) {
                return removeEdgeOnVertexDelete(id);
            }
        });
    }

    // This method is called to remove an edge through the IEdgeRemover
    private IEdge removeEdgeOnVertexDelete(UUID edgeId) throws BitsyException {
        // This is called from remove on adjMap, which means that the edge was added in this Tx
        BitsyEdge edge = changedEdges.remove(edgeId);

        // Only an edge that is present in this Tx can be removed by the IEdgeRemover
        assert (edge != null);

        return edge;
    }

    public void addTransactionListener(Consumer<Status> listener) {
        transactionListeners.add(listener);
    }

    public void removeTransactionListener(Consumer<Status> listener) {
        transactionListeners.remove(listener);
    }

    public void clearTransactionListeners() {
        transactionListeners.clear();
    }

    public void announceCommit(BitsyTransaction t) {
        this.transactionListeners.forEach(c -> c.accept(Status.COMMIT));
    }

    public void announceRollback(BitsyTransaction t) {
        this.transactionListeners.forEach(c -> c.accept(Status.ROLLBACK));
    }

    public void onReadWrite(Consumer<Transaction> consumer) {
        readWriteConsumer =
                Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onReadWriteBehaviorCannotBeNull);
    }

    public void onClose(Consumer<Transaction> consumer) {
        closeConsumer = Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onCloseBehaviorCannotBeNull);
    }

    public Consumer<Transaction> getReadWriteConsumer() {
        return readWriteConsumer;
    }

    public Consumer<Transaction> getCloseConsumer() {
        return closeConsumer;
    }

    public void clear() {
        unmodifiedVertices.clear();
        unmodifiedEdges.clear();
        changedVertices.clear();
        changedEdges.clear();
        adjMap.clear();

        // Don't clear the long-lived subscriptions, viz. transactionListeners, readWriteConsumer and closeConsumer
    }
}
