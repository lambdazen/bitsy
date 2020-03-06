package com.lambdazen.bitsy;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdazen.bitsy.gremlin.BitsyTraversalStrategy;
import com.lambdazen.bitsy.store.FileBackedMemoryGraphStore;
import com.lambdazen.bitsy.store.MemoryGraphStore;
import com.lambdazen.bitsy.tx.BitsyTransaction;
import com.lambdazen.bitsy.tx.BitsyTransactionContext;
import com.lambdazen.bitsy.wrapper.BitsyAutoReloadingGraph;

@Graph.OptIn("com.lambdazen.bitsy.structure.BitsyGraphStructureTestSuite")
@Graph.OptIn("com.lambdazen.bitsy.structure.BitsyProcessStandardTestSuite")
/** Bitsy 3.0 compatible with Tinkerpop 3.0 */
public class BitsyGraph implements Graph, BitsyGraphMBean {
    private static final Logger log = LoggerFactory.getLogger(BitsyGraph.class);

    public static boolean IS_ANDROID = "The Android Project".equals(System.getProperty("java.specification.vendor"));

    // Configuration keys
    public static final String DB_PATH_KEY = "dbPath";
    public static final String ALLOW_FULL_GRAPH_SCANS_KEY = "allowFullGraphScans";
    public static final String DEFAULT_ISOLATION_LEVEL_KEY = "defaultIsolationLevel";
    public static final String TX_LOG_THRESHOLD_KEY = "txLogThreshold";
    public static final String REORG_FACTOR_KEY = "reorgFactor";
    public static final String CREATE_DIR_IF_MISSING_KEY = "createDirIfMissing";
    public static final String VERTEX_INDICES_KEY = "vertexIndices";
    public static final String EDGE_INDICES_KEY = "edgeIndices";

    public static final double DEFAULT_REORG_FACTOR = 1;
    public static final long DEFAULT_TX_LOG_THRESHOLD = 4 * 1024 * 1024;

    private boolean allowFullGraphScans;
    private boolean isPersistent;
    private Path dbPath;
    private ThreadLocal<BitsyTransaction> curTransaction;
    private ThreadLocal<BitsyTransactionContext> curTransactionContext;
    private IGraphStore graphStore;
    private Features bitsyFeatures;
    private ObjectName objectName;
    private BitsyIsolationLevel defaultIsolationLevel;
    private boolean createDirIfMissing = false;
    private Configuration origConfig;

    static {
        try {
            TraversalStrategies.GlobalCache.registerStrategies(BitsyGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(BitsyTraversalStrategy.instance()));
            TraversalStrategies.GlobalCache.registerStrategies(BitsyAutoReloadingGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(BitsyTraversalStrategy.instance()));
        } catch (java.lang.BootstrapMethodError e) {
            // Known issue with Android
            System.err.println("Not registering traversal strategies");
            e.printStackTrace();
        }
    }

    // Protected constructor used by ThreadedBitsyGraph
    protected BitsyGraph(char isThreaded, boolean allowFullGraphScans) {
        // char isThreaded is used to distinguish this constructor from others
        this.allowFullGraphScans = allowFullGraphScans;
    }

    public BitsyGraph() {
        this(true);
    }

    public BitsyGraph(boolean allowFullGraphScans) {
        this(null, true, -1, -1);
    }

    public BitsyGraph(Path dbPath) {
        this(dbPath, true, DEFAULT_TX_LOG_THRESHOLD, DEFAULT_REORG_FACTOR); // Default tx log size is 4MB
    }

    /**
     * Constructor with all configurable parameters
     * @param dbPath path to the database files
     * @param allowFullGraphScans whether/not iterations on vertices and edges should be supported
     * @param txLogThreshold the size of the transaction in bytes after which it will be scheduled to move to V/E files
     * @param reorgFactor V/E reorgs are triggered when the size of the V/E files exceeds the initial size by (1 + factor)
     */
    public BitsyGraph(Path dbPath, boolean allowFullGraphScans, long txLogThreshold, double reorgFactor) {
        this(dbPath, allowFullGraphScans, txLogThreshold, reorgFactor, false);
    }

    /**
     * Constructor with all configurable parameters
     * @param dbPath path to the database files
     * @param allowFullGraphScans whether/not iterations on vertices and edges should be supported
     * @param txLogThreshold the size of the transaction in bytes after which it will be scheduled to move to V/E files
     * @param reorgFactor V/E reorgs are triggered when the size of the V/E files exceeds the initial size by (1 + factor)
     * @param createDirIfMissing create the Bitsy directory if it is missing
     */
    public BitsyGraph(Path dbPath, boolean allowFullGraphScans, long txLogThreshold, double reorgFactor, boolean createDirIfMissing) {
        this.dbPath = dbPath;
        this.allowFullGraphScans = allowFullGraphScans;
        this.curTransactionContext = new ThreadLocal<BitsyTransactionContext>();
        this.curTransaction = new ThreadLocal<BitsyTransaction>();
        this.defaultIsolationLevel = BitsyIsolationLevel.READ_COMMITTED;
        this.createDirIfMissing = createDirIfMissing;

        if (IS_ANDROID) {
                if (isPersistent()) {
                    // Load from files
                    this.graphStore = new FileBackedMemoryGraphStore(new MemoryGraphStore(allowFullGraphScans), dbPath, txLogThreshold, reorgFactor, createDirIfMissing);
                } else {
                    this.graphStore = new MemoryGraphStore(allowFullGraphScans);
                }
        } else {
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                if (isPersistent()) {
                    // Make sure that another BitsyGraph doesn't exist with the same path
                    try {
                        this.objectName = new ObjectName("com.lambdazen.bitsy", "path", ObjectName.quote(dbPath.toString()));
                    } catch (MalformedObjectNameException e) {
                        throw new BitsyException(BitsyErrorCodes.INTERNAL_ERROR, "Bug in quoting ObjectName", e);
                    }

                    // Check registry
                    if (server.isRegistered(objectName)) {
                        throw new BitsyException(BitsyErrorCodes.INSTANCE_ALREADY_EXISTS, "Path " + dbPath.toString());
                    }

                    // Load from files
                    this.graphStore = new FileBackedMemoryGraphStore(new MemoryGraphStore(allowFullGraphScans), dbPath, txLogThreshold, reorgFactor, createDirIfMissing);
                } else {
                    this.graphStore = new MemoryGraphStore(allowFullGraphScans);
                }

                // Register this to the MBeanServer
                if (objectName != null) {
                    try {
                        server.registerMBean(this, objectName);
                    } catch (Exception e) {
                        throw new BitsyException(BitsyErrorCodes.ERROR_REGISTERING_TO_MBEAN_SERVER, "Encountered exception", e);
                    }
                }
        }

        this.bitsyFeatures = new BitsyFeatures(isPersistent);
    }

    /**
     * Constructor with a Configuration object with String dbPath, boolean allowFullGraphScans, long txLogThreshold and double reorgFactor
     */
    public BitsyGraph(Configuration configuration) {
        this(Paths.get(configuration.getString(DB_PATH_KEY)),
                configuration.getBoolean(ALLOW_FULL_GRAPH_SCANS_KEY, Boolean.TRUE),
                configuration.getLong(TX_LOG_THRESHOLD_KEY, DEFAULT_TX_LOG_THRESHOLD),
                configuration.getDouble(REORG_FACTOR_KEY, DEFAULT_REORG_FACTOR),
                configuration.getBoolean(CREATE_DIR_IF_MISSING_KEY, false));
        String isoLevelStr = configuration.getString(DEFAULT_ISOLATION_LEVEL_KEY);
        if (isoLevelStr != null) {
                setDefaultIsolationLevel(BitsyIsolationLevel.valueOf(isoLevelStr));
        }
        String vertexIndices = configuration.getString(VERTEX_INDICES_KEY);
        if (vertexIndices != null) {
                createIndices(Vertex.class, vertexIndices);
        }
        String edgeIndices = configuration.getString(EDGE_INDICES_KEY);
        if (edgeIndices != null) {
                createIndices(Edge.class, edgeIndices);
        }
        this.origConfig = configuration;
    }

        private void createIndices(Class elemType, String vertexIndices) {
                for (String indexKey : vertexIndices.split(",")) {
                        try {
                                createKeyIndex(indexKey.trim(), elemType);
                        } catch (BitsyException ex) {
                                if (ex.getErrorCode() == BitsyErrorCodes.INDEX_ALREADY_EXISTS) {
                                        // That's fine
                                } else {
                                        throw ex;
                                }
                        }
                }
        }

    public static final BitsyGraph open(Configuration configuration) {
        return new BitsyGraph(configuration);
    }

    public String toString() {
        if (dbPath != null) {
            return "bitsygraph[" + dbPath + "]";
        } else {
            return "bitsygraph[<in-memory>]";
        }
    }

    /** This method can be used to check if the current thread has an ongoing transaction */
    public boolean isTransactionActive() {
        ITransaction tx = curTransaction.get();

        return (tx != null);
    }

    public boolean isPersistent() {
        return (dbPath != null);
    }

    public boolean isFullGraphScanAllowed() {
        return allowFullGraphScans;
    }

    public BitsyIsolationLevel getDefaultIsolationLevel() {
        return defaultIsolationLevel;
    }

    public void setDefaultIsolationLevel(BitsyIsolationLevel level) {
        this.defaultIsolationLevel = level;
    }

    public BitsyIsolationLevel getTxIsolationLevel() {
        return getTx().getIsolationLevel();
    }

    public void setTxIsolationLevel(BitsyIsolationLevel level) {
        getTx().setIsolationLevel(level);
    }

    public double getReorgFactor() {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Reorg factor is only defined for persistent graphs (with a defined path to DB)");
        } else {
            return ((FileBackedMemoryGraphStore)graphStore).getVEReorgPotential().getFactor();
        }
    }

    public void setReorgFactor(double factor) {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Reorg factor is only defined for persistent graphs (with a defined path to DB)");
        } else {
            ((FileBackedMemoryGraphStore)graphStore).getVEReorgPotential().setFactor(factor);
        }
    }

    public int getMinLinesPerReorg() {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Reorg factor is only defined for persistent graphs (with a defined path to DB)");
        } else {
            return ((FileBackedMemoryGraphStore)graphStore).getVEReorgPotential().getMinLinesPerReorg();
        }
    }

    public void setMinLinesPerReorg(int minLinesPerReorg) {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Reorg factor is only defined for persistent graphs (with a defined path to DB)");
        } else {
            ((FileBackedMemoryGraphStore)graphStore).getVEReorgPotential().setMinLinesPerReorg(minLinesPerReorg);
        }
    }

    public long getTxLogThreshold() {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Transaction log threshold is only defined for persistent graphs (with a defined path to DB)");
        } else {
            return ((FileBackedMemoryGraphStore)graphStore).getTxLogFlushPotential().getTxLogThreshold();
        }
    }

    public void setTxLogThreshold(long txLogThreshold) {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Transaction log threshold is only defined for persistent graphs (with a defined path to DB)");
        } else {
            ((FileBackedMemoryGraphStore)graphStore).getTxLogFlushPotential().setTxLogThreshold(txLogThreshold);
        }
    }

    /** This method flushes the transaction log to the V/E text files */
    public void flushTxLog() {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Transaction log threshold is only defined for persistent graphs (with a defined path to DB)");
        } else {
            ((FileBackedMemoryGraphStore)graphStore).flushTxLog();
        }
    }

    /** This method backs up the database while it is still operational. Only one backup can be in progress at a time.
     *
     * @param pathToDir directory to which the database must be backed up.
     */
    public void backup(String pathToDir) {
        backup(Paths.get(pathToDir));
    }

    /** This method backs up the database while it is still operational. Only one backup can be in progress at a time.
     *
     * @param pathToDir directory to which the database must be backed up.
     */
    public void backup(Path pathToDir) {
        if (!isPersistent()) {
            throw new BitsyException(BitsyErrorCodes.OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS, "Transaction log threshold is only defined for persistent graphs (with a defined path to DB)");
        } else {
            ((FileBackedMemoryGraphStore)graphStore).backup(pathToDir);
        }
    }

    protected BitsyTransaction getTx() {
        BitsyTransaction tx = curTransaction.get();

        if ((tx == null) || !tx.isOpen()) {
            BitsyTransactionContext txContext = curTransactionContext.get();

            if (txContext == null) {
                txContext = new BitsyTransactionContext(graphStore);
                curTransactionContext.set(txContext);
            }

            tx = new BitsyTransaction(txContext, defaultIsolationLevel, this);

            curTransaction.set(tx);
        }

        return tx;
    }

    @Override
    public ITransaction tx() {
        return getTx();
    }

    /* UNIMPLEMENTED OLAP METHODS */
    @Override
    public GraphComputer compute() {
        throw new UnsupportedOperationException("Bitsy doesn't support the compute() method", new BitsyException(BitsyErrorCodes.NO_OLAP_SUPPORT));
    }

    @Override
    public GraphComputer compute(Class graphComputerClass) {
        throw new UnsupportedOperationException("Bitsy doesn't support the compute() method", new BitsyException(BitsyErrorCodes.NO_OLAP_SUPPORT));
    }

    @Override
    public <I extends Io> I io(final Io.Builder<I> builder) {
        return (I) builder.graph(this).onMapper(m -> m.addRegistry(BitsyIoRegistryV3d0.instance())).create();
    }

    /* FEATURES */
    @Override
    public Graph.Features features() {
        return bitsyFeatures;
    }

    /* CONFIGURATION */
    @Override
    public Configuration configuration() {
        if (this.origConfig != null) {
                return this.origConfig;
        } else {
            Configuration ans = new BaseConfiguration();
            ans.setProperty(DB_PATH_KEY, dbPath.toString());
            ans.setProperty(ALLOW_FULL_GRAPH_SCANS_KEY, allowFullGraphScans);
            ans.setProperty(DEFAULT_ISOLATION_LEVEL_KEY, defaultIsolationLevel.toString());
            ans.setProperty(TX_LOG_THRESHOLD_KEY, getTxLogThreshold());
            ans.setProperty(REORG_FACTOR_KEY, getReorgFactor());
            ans.setProperty(CREATE_DIR_IF_MISSING_KEY, createDirIfMissing);

            ans.setProperty(VERTEX_INDICES_KEY, String.join(",", getIndexedKeys(Vertex.class)));
            ans.setProperty(EDGE_INDICES_KEY, String.join(",", getIndexedKeys(Vertex.class)));

            return ans;
        }
    }

    private void validateHomogenousIds(final Object[] ids) {
        final Class firstClass = ids[0].getClass();
        for (int i=1; i < ids.length; i++) {
                Class curClass = ids[i].getClass();
                if (!curClass.equals(firstClass)) {
                        throw new IllegalArgumentException("Argument " + i  + " has class " + curClass + " which mismatches arg 0's class " + firstClass);
                }
        }
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        if (keyValues == null) {
                throw new IllegalArgumentException("Expecting non-null arguments in addVertex");
        } else if (keyValues.length % 2 == 1) {
            throw new IllegalArgumentException("Expecting even number of items in the keyValue array. Found " + keyValues.length);
        }

        // Validate first
        for (int i = 0; i < keyValues.length; i = i+2) {
                if (keyValues[i] == T.id) {
                // We don't support custom IDs
                throw new UnsupportedOperationException("Encountered T.id in addVertex", new BitsyException(BitsyErrorCodes.NO_CUSTOM_ID_SUPPORT));
                } else if (keyValues[i] == null) {
                        throw new IllegalArgumentException("Encountered a null key in argument #" + i);
                } else if (keyValues[i+1] == null) {
                        throw new IllegalArgumentException("Encountered a null value in argument #" + i);
                } else if (keyValues[i] == T.label) {
                        // That's fine
                } else if (!(keyValues[i] instanceof String)) {
                        throw new IllegalArgumentException("Encountered a non-string key: " + keyValues[i] + " in argument #" + i);
                }
        }

        // Do the work
        final String label = ElementHelper.getLabelValue(keyValues).orElse(null);
        BitsyTransaction tx = getTx();
        BitsyVertex vertex = new BitsyVertex(UUID.randomUUID(), label, null, tx, BitsyState.M, 0);

        for (int i = 0; i < keyValues.length; i = i+2) {
                if (keyValues[i] == T.label) {
                        // Already found it
                } else {
                        String key = (String)keyValues[i];
                        vertex.property(key, keyValues[i+1]);
                }
        }

        tx.addVertex(vertex);

        return vertex;
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        if (vertexIds.length == 0) {
            if (!allowFullGraphScans) {
                throw new BitsyException(BitsyErrorCodes.FULL_GRAPH_SCANS_ARE_DISABLED, "Can not evaluate vertices()");
            }

            final ITransaction tx = getTx();

            return tx.getAllVertices();
        } else if (vertexIds.length == 1 ) {
            Vertex vertex = getVertex(vertexIds[0]);
            if (vertex == null) {
                return Collections.<Vertex>emptyList().iterator();
            } else {
                return Collections.singletonList(vertex).iterator();
            }
        } else {
                validateHomogenousIds(vertexIds);
            List<Vertex> ans = new ArrayList<Vertex>();
            for (Object vertexId : vertexIds) {
                Vertex vertex = getVertex(vertexId);
                if (vertex != null) {
                    ans.add(vertex);
                }
            }
            return ans.iterator();
        }
    }

    private Vertex getVertex(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("The vertex ID passed to getVertex() is null");
        }

        Vertex ans;
        if (id instanceof UUID) {
            ans = getTx().getVertex((UUID)id);
        } else if (id instanceof String) {
            // Get the UUID from the string representation -- may fail
            UUID uuid;
            try {
                uuid = UUID.fromString((String)id);
            } catch (IllegalArgumentException e) {
                // Decoding failed
                return null;
            }

            ans = getTx().getVertex(uuid);
        } else if (id instanceof Vertex) {
                return getTx().getVertex((UUID)((Vertex)id).id());
        } else {
            // Wrong type
            ans = null;
        }

        return ans;
    }

    private Edge getEdge(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("The edge ID passed to getEdge() is null");
        }

        if (id instanceof UUID) {
            return getTx().getEdge((UUID)id);
        } else if (id instanceof String) {
            // Get the UUID from the string representation -- may fail
            UUID uuid;
            try {
                uuid = UUID.fromString((String)id);
            } catch (IllegalArgumentException e) {
                // Decoding failed
                return null;
            }

            return getTx().getEdge(uuid);
        } else if (id instanceof Edge) {
                return getTx().getEdge((UUID)((Edge)id).id());
        } else {
            // Wrong type
            return null;
        }
    }

    public Iterator<Edge> edges(Object... edgeIds) {
        if (edgeIds.length == 0) {
            if (!allowFullGraphScans) {
                throw new BitsyException(BitsyErrorCodes.FULL_GRAPH_SCANS_ARE_DISABLED, "Can not evaluate edges()");
            }

            final ITransaction tx = getTx();

            return tx.getAllEdges();
        } else if (edgeIds.length == 1 ) {
            Edge edge = getEdge(edgeIds[0]);
            if (edge == null) {
                return Collections.<Edge>emptyList().iterator();
            } else {
                return Collections.singletonList(edge).iterator();
            }
        } else {
                validateHomogenousIds(edgeIds);
            List<Edge> ans = new ArrayList<Edge>();
            for (Object edgeId : edgeIds) {
                Edge edge = getEdge(edgeId);

                if (edge != null) {
                    ans.add(edge);
                }
            }
            return ans.iterator();
        }
    }

    public void shutdown() {
        try {
            // As per Blueprints tests, shutdown() implies automatic commit
                BitsyTransaction tx = curTransaction.get();
                if ((tx != null) && tx.isOpen()) {
                        tx.commit();
                        tx = null;
                }

            // Shutdown the underlying store
            graphStore.shutdown();
        } finally {
            if (this.objectName != null) {
                // Deregister from JMX
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                try {
                    server.unregisterMBean(objectName);
                } catch (Exception e) {
                    log.error("Error unregistering MBean named " + objectName + " from the MBeanServer", e);
                }
                objectName = null;
            }
        }
    }

    // Key indexes from TP2 -- now requires TraversalStrategy
    public <T extends Element> void createKeyIndex(String key, Class<T> elementType) {
        graphStore.createKeyIndex(key, elementType);
    }

    public <T extends Element> void dropKeyIndex(String key, Class<T> elementType) {
        graphStore.dropKeyIndex(key, elementType);
    }

    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementType) {
        return graphStore.getIndexedKeys(elementType);
    }

    public Iterator<BitsyVertex> verticesByIndex(final String key, final Object value) {
        final ITransaction tx = getTx();

        return tx.lookupVertices(key, value);
    }

    public Iterator<BitsyEdge> edgesByIndex(final String key, final Object value) {
        final ITransaction tx = getTx();

        return tx.lookupEdges(key, value);
    }

    public IGraphStore getStore() {
        return graphStore;
    }

        @Override
        public void close() throws Exception {
                this.shutdown();
        }

        @Override
        public Variables variables() {
                throw new UnsupportedOperationException("Bitsy doesn't support variables. Please store the data in a vertex");
        }
}
