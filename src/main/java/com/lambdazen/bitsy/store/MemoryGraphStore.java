package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyErrorCodes;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyRetryException;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.ICommitChanges;
import com.lambdazen.bitsy.IEdge;
import com.lambdazen.bitsy.IGraphStore;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.index.EdgeIndexMap;
import com.lambdazen.bitsy.index.VertexIndexMap;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * This class implements a MapDB-backed store for a graph, along with its key
 * indexes.
 */
public class MemoryGraphStore implements IGraphStore {
    //    private static final Logger log = LoggerFactory.getLogger(MemoryGraphStore.class);

    private static final int MAX_COUNTER_INCREASE_BEFORE_READ_LOCK =
            2 * 5; // After 5 writes go through, the reader will lock
    private static final int MAX_RETRIES_BEFORE_READ_LOCK = 3; // After 3 writes go through, the reader will lock

    private ReadWriteLock rwLock;

    private AtomicLong spinCounter;

    private Map<UUID, VertexBean> vertices;

    // Map from VertexBean.getUUID().hashCode() -> VertexBean
    private Map<UUID, EdgeBean> edges;

    private AdjacencyMapForBeans adjMap;
    private VertexIndexMap vIndexMap;
    private EdgeIndexMap eIndexMap;
    private boolean allowFullGraphScans;

    public MemoryGraphStore(boolean allowFullGraphScans) {
        this.rwLock = new ReentrantReadWriteLock(true);
        this.allowFullGraphScans = allowFullGraphScans;
        this.spinCounter = new AtomicLong(0);

        reset();
    }

    protected void reset() {
        this.vertices = new ConcurrentHashMap<UUID, VertexBean>();
        this.edges = new ConcurrentHashMap<UUID, EdgeBean>();

        this.adjMap = new AdjacencyMapForBeans(false, new IEdgeRemover() {
            @Override
            public IEdge removeEdge(UUID id) {
                return edges.remove(id);
            }
        });
        this.vIndexMap = new VertexIndexMap();
        this.eIndexMap = new EdgeIndexMap();
    }

    @Override
    public boolean allowFullGraphScans() {
        return allowFullGraphScans;
    }

    /**
     * This method commits a set of changes. It requires a write lock on the Map
     * DB.
     */
    public void commit(ICommitChanges changes) {
        commit(changes, true, null);
    }

    // This method is called with incrementVersions=false from
    // FileBackedMemoryGraphStore
    public void commit(ICommitChanges changes, boolean incrementVersions, Runnable r) {
        beginWrite();

        try {
            checkForConcurrentModifications(changes, incrementVersions);

            saveChanges(changes);

            if (r != null) {
                r.run();
            }
        } finally {
            endWrite();
        }
    }

    private void beginWrite() {
        rwLock.writeLock().lock();

        // There is a possibility that the counter is odd. This happens when a
        // writer thread is done unlocking, but not done incrementing the
        // counter. Calling beginRead() ensures that the counter is even
        RetryDetails retryDetails = new RetryDetails();
        beginRead(retryDetails, false);

        assert (retryDetails.counter & 1L) == 0L : "Bug in beginRead -- counter is odd!"; // must be even
        long newCounter = (retryDetails.counter + 1) & 0x3fffffffffffffffL; // Don't let it go negative

        boolean updated = spinCounter.compareAndSet(retryDetails.counter, newCounter);
        assert updated : "Someone messed with an even counter without a lock!";
    }

    private void endWrite() {
        // Unlock first -- this creates the synchronization barrier ensuring
        // that all writes are visible to readers
        rwLock.writeLock().unlock();

        // Now increment the counter allowing readers & writers to proceed
        // At this point -- both readers and writers will wait till the counter
        // turns even
        long counter = spinCounter.get();
        assert (counter & 1L) == 1L : "Some writer did not leave the counter odd!"; // must be odd
        long newCounter = (counter + 1) & 0x3fffffffffffffffL; // Don't let it go negative

        boolean updated = spinCounter.compareAndSet(counter, newCounter);
        assert updated : "Someone messed with the counter without a lock!";
    }

    private void beginRead(RetryDetails retryDetails, boolean degradeToReadLock) {
        // Counter can't be -1 because the read lock will always succeed
        assert (retryDetails.counter != -1);

        while ((retryDetails.counter & 1L) != 0L) {
            long tryCount = retryDetails.counter - retryDetails.startCounter;

            // The counter is odd -- which means that a write is in process
            if (degradeToReadLock
                    && ((tryCount > MAX_COUNTER_INCREASE_BEFORE_READ_LOCK)
                            || (retryDetails.retryCount > MAX_RETRIES_BEFORE_READ_LOCK))) {
                rwLock.readLock().lock();

                retryDetails.counter = -1;
                return;
            }

            // No work left for this thread
            Thread.yield();

            // Try again
            retryDetails.counter = spinCounter.get();
        }

        assert ((retryDetails.counter & 1L) == 0L);
    }

    private void endRead(RetryDetails retryDetails) {
        if (retryDetails.counter == -1) {
            rwLock.readLock().unlock();
        }
    }

    private boolean shouldRetryRead(RetryDetails retryDetails) {
        // Retry if there is no read lock AND the counter doesn't match
        if (retryDetails.counter == -1) {
            return false;
        }

        long counter = spinCounter.get();
        if (counter == retryDetails.counter) {
            return false;
        } else {
            // Need to retry -- but keep track of the new counter
            retryDetails.counter = counter;
            retryDetails.retryCount++;
            return true;
        }
    }

    private void checkForConcurrentModifications(ICommitChanges changes, boolean incrementVersions) {
        // Check the versions
        for (BitsyVertex vertex : changes.getVertexChanges()) {
            if (incrementVersions) {
                // Increment the version before the commit
                vertex.incrementVersion();
            }

            UUID key = (UUID) vertex.id();

            switch (vertex.getState()) {
                case U:
                    break;

                case D:
                case M:
                    VertexBean vb = vertices.get(key);
                    if (vb != null && (vb.getVersion() + 1 != vertex.getVersion())) {
                        throw new BitsyRetryException(
                                BitsyErrorCodes.CONCURRENT_MODIFICATION,
                                "Vertex " + key + " was modified. Loaded version " + (vertex.getVersion() - 1)
                                        + ". Current version in DB: " + vb.getVersion());
                    }
            }
        }

        // Check the versions of edge next
        for (BitsyEdge edge : changes.getEdgeChanges()) {
            if (incrementVersions) {
                // Increment the version before the commit
                edge.incrementVersion();
            }

            UUID key = (UUID) edge.id();

            switch (edge.getState()) {
                case U:
                    break;

                case D:
                case M:
                    EdgeBean eb = edges.get(key);
                    if (eb != null && (eb.getVersion() + 1) != edge.getVersion()) {
                        throw new BitsyRetryException(
                                BitsyErrorCodes.CONCURRENT_MODIFICATION,
                                "Edge " + key + " was modified. Loaded version " + (edge.getVersion() - 1)
                                        + ". Current version in DB: " + eb.getVersion());
                    }
            }
        }
    }

    protected long saveChanges(ICommitChanges changes) {
        return saveChanges(changes, null);
    }

    // This method is used by commit (with increment option) and the initial
    // load from DB (without increment option)
    protected long saveChanges(ICommitChanges changes, IStringCanonicalizer canonicalizer) {
        long addedVE = 0;

        // All OK. Update vertex
        for (BitsyVertex vertex : changes.getVertexChanges()) {
            addedVE = saveVertex(addedVE, vertex, canonicalizer);
        }

        // Process the edges next
        for (BitsyEdge edge : changes.getEdgeChanges()) {
            addedVE = saveEdge(addedVE, edge, canonicalizer);
        }

        return addedVE;
    }

    protected long saveEdge(long addedVE, BitsyEdge edge, IStringCanonicalizer canonicalizer) {
        UUID key = (UUID) edge.id();

        switch (edge.getState()) {
            case U:
                break;

            case D:
                eIndexMap.remove(edges.get(key));
                // log.debug("Removing edge {}", edge.getId());

                // Remove this edge from incoming and outgoing vertices
                EdgeBean eBeanToRemove = edges.remove(key);
                adjMap.removeEdgeWithoutCallback(eBeanToRemove);
                addedVE--;

                break;

            case M:
                EdgeBean eBean = (canonicalizer == null) ? asBean(edge) : asBean(edge, canonicalizer);
                if (eBean == null) {
                    // log.debug("Skipping edge {}", edge.getId());
                } else {
                    // log.debug("Modifying edge {}", edge.getId());
                    EdgeBean oldEBean = edges.get(key);
                    eIndexMap.remove(oldEBean);
                    eIndexMap.add(eBean);

                    EdgeBean oldEBean2 = edges.put(eBean, eBean);

                    // NOTE: Because this is a write operation, there is an
                    // exclusive lock -- no one else is updating eIndexMap
                    assert (oldEBean == oldEBean2);

                    if (oldEBean != null) {
                        adjMap.removeEdgeWithoutCallback(oldEBean); // Don't callback
                    } else {
                        addedVE++;
                    }

                    adjMap.addEdge(eBean);
                }
        }
        return addedVE;
    }

    protected long saveVertex(long addedVE, BitsyVertex vertex, IStringCanonicalizer canonicalizer) {
        UUID key = (UUID) vertex.id();

        switch (vertex.getState()) {
            case U:
                break;

            case D:
                // log.debug("Deleting vertex {}", key);
                vIndexMap.remove(vertices.get(key));
                VertexBean vBeanToRemove = vertices.remove(key);
                adjMap.removeVertex(vBeanToRemove);
                addedVE--;

                break;

            case M:
                // log.debug("Updating vertex {}", key);
                VertexBean vBean = (canonicalizer == null) ? vertex.asBean() : vertex.asBean(canonicalizer);
                VertexBean oldVBean = vertices.get(key);
                vIndexMap.remove(oldVBean);

                if (oldVBean == null) {
                    vertices.put(vBean, vBean);
                    vIndexMap.add(vBean);
                    addedVE++;
                } else {
                    oldVBean.copyFrom(vBean);
                    vIndexMap.add(oldVBean);
                }
        }
        return addedVE;
    }

    public VertexBean getVertex(UUID id) {
        // This method is only for internal use
        // Not using a read lock because the ID is available
        return vertices.get(id);
    }

    public EdgeBean getEdge(UUID id) {
        // This method is only for internal use
        // Not using a read lock because the ID is available
        return edges.get(id);
    }

    @Override
    public BitsyVertex getBitsyVertex(BitsyTransaction tx, UUID id) {
        RetryDetails retryDetails = new RetryDetails();
        BitsyVertex ans = null;

        try {
            do {
                beginRead(retryDetails, true);

                VertexBean bean = getVertex(id);
                if (bean != null) {
                    ans = new BitsyVertex(bean, tx, BitsyState.U);
                }
            } while (shouldRetryRead(retryDetails));
        } finally {
            endRead(retryDetails);
        }

        return ans;
    }

    @Override
    public BitsyEdge getBitsyEdge(BitsyTransaction tx, UUID id) {
        RetryDetails retryDetails = new RetryDetails();
        BitsyEdge ans = null;

        try {
            do {
                beginRead(retryDetails, true);

                EdgeBean bean = getEdge(id);
                if (bean != null) {
                    ans = new BitsyEdge(bean, tx, BitsyState.U);
                }

            } while (shouldRetryRead(retryDetails));
        } finally {
            endRead(retryDetails);
        }

        return ans;
    }

    public List<EdgeBean> getEdges(UUID vertexId, Direction dir, String[] edgeLabels) {
        RetryDetails retryDetails = new RetryDetails();
        List<EdgeBean> ans;

        try {
            do {
                beginRead(retryDetails, true);
                VertexBean vBean = vertices.get(vertexId);

                ans = adjMap.getEdges(vBean, dir, edgeLabels);
            } while (shouldRetryRead(retryDetails));
        } finally {
            endRead(retryDetails);
        }

        return ans;
    }

    @Override
    public Collection<VertexBean> getAllVertices() {
        // This method exposes the underlying collection, without a read/write
        // lock. The idea is to not block other operations while a reader is
        // cycling through all the vertices. There returned list may include
        // some vertices from ongoing transaction, but not others.

        return vertices.values();
    }

    @Override
    public Collection<EdgeBean> getAllEdges() {
        return edges.values();
    }

    @Override
    public <T extends Element> void createKeyIndex(String key, Class<T> elementType) {
        beginWrite();

        try {
            if (elementType == null) {
                throw new IllegalArgumentException("Element type in createKeyIndex() can not be null");
            } else if (elementType.equals(Vertex.class)) {
                vIndexMap.createKeyIndex(key, getAllVertices().iterator());
            } else if (elementType.equals(Edge.class)) {
                eIndexMap.createKeyIndex(key, getAllEdges().iterator());
            } else {
                throw new BitsyException(
                        BitsyErrorCodes.UNSUPPORTED_INDEX_TYPE, "Encountered index type: " + elementType);
            }
        } finally {
            endWrite();
        }
    }

    @Override
    public <T extends Element> void dropKeyIndex(String key, Class<T> elementType) {
        beginWrite();

        try {
            if (elementType == null) {
                throw new IllegalArgumentException("Element type in dropKeyIndex() can not be null");
            } else if (elementType.equals(Vertex.class)) {
                vIndexMap.dropKeyIndex(key);
            } else if (elementType.equals(Edge.class)) {
                eIndexMap.dropKeyIndex(key);
            } else {
                throw new BitsyException(
                        BitsyErrorCodes.UNSUPPORTED_INDEX_TYPE, "Encountered index type: " + elementType);
            }
        } finally {
            endWrite();
        }
    }

    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementType) {
        // Getting a write lock because this method accesses the list of index names which isn't thread-safe
        beginWrite();

        try {
            if (elementType == null) {
                throw new IllegalArgumentException("Element type in getIndexedKeys() can not be null");
            } else if (elementType.equals(Vertex.class)) {
                return vIndexMap.getIndexedKeys();
            } else if (elementType.equals(Edge.class)) {
                return eIndexMap.getIndexedKeys();
            } else {
                throw new BitsyException(
                        BitsyErrorCodes.UNSUPPORTED_INDEX_TYPE, "Encountered index type: " + elementType);
            }
        } finally {
            endWrite();
        }
    }

    @Override
    public Collection<VertexBean> lookupVertices(String key, Object value) {
        RetryDetails retryDetails = new RetryDetails();
        Collection<VertexBean> ans;

        try {
            do {
                beginRead(retryDetails, true);
                ans = vIndexMap.get(key, value);
            } while (shouldRetryRead(retryDetails));
        } finally {
            endRead(retryDetails);
        }

        return ans;
    }

    @Override
    public Collection<EdgeBean> lookupEdges(String key, Object value) {
        RetryDetails retryDetails = new RetryDetails();
        Collection<EdgeBean> ans;

        try {
            do {
                beginRead(retryDetails, true);
                ans = eIndexMap.get(key, value);
            } while (shouldRetryRead(retryDetails));
        } finally {
            endRead(retryDetails);
        }

        return ans;
    }

    @Override
    public void shutdown() {
        reset();
    }

    // HELPER METHODS
    public EdgeBean asBean(BitsyEdge edge) {
        // The TX is usually not active at this point. So no checks.
        VertexBean outVertexBean = getVertex(edge.getOutVertexId());
        VertexBean inVertexBean = getVertex(edge.getInVertexId());

        if ((outVertexBean == null) || (inVertexBean == null)) {
            // The vertex has been deleted.
            return null;
        } else {
            assert (edge.getState() == BitsyState.M);
            return new EdgeBean(
                    (UUID) edge.id(),
                    edge.getPropertyDict(),
                    edge.getVersion(),
                    edge.label(),
                    outVertexBean,
                    inVertexBean);
        }
    }

    public EdgeBean asBean(BitsyEdge edge, IStringCanonicalizer canonicalizer) {
        EdgeBean ans = asBean(edge);

        if (ans != null) {
            // Canonicalize the label
            ans.label = (ans.label == null) ? null : canonicalizer.canonicalize(ans.label);
        }

        if (edge.getPropertyDict() != null) {
            edge.getPropertyDict().canonicalizeKeys(canonicalizer);
        }

        return ans;
    }

    // Retry details
    public class RetryDetails {
        long counter;
        long startCounter;
        int retryCount;

        public RetryDetails() {
            this.startCounter = spinCounter.get();
            this.counter = startCounter;
            this.retryCount = 0;
        }
    }
}
