package com.lambdazen.bitsy;

import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;

import com.lambdazen.bitsy.tx.BitsyTransaction;
import com.lambdazen.bitsy.tx.BitsyTransactionContext;

public class ThreadedBitsyGraph extends BitsyGraph {
    BitsyGraph underlyingGraph;
    BitsyTransaction tx;
    
    public ThreadedBitsyGraph(BitsyGraph g) {
        // Using protected constructor that doesn't create a graph store
        super('_', g.isFullGraphScanAllowed());
        
        this.underlyingGraph = g;
        this.tx = null;
    }
    
    public String toString() {
        return underlyingGraph.toString();
    }

    @Override
    public Features features() {
        return underlyingGraph.features();
    }
    
    @Override
    protected BitsyTransaction getTx() {
        // Overriding the getTx() method ensures that the work will be done on
        // the local transaction, NOT the ThreadLocal transaction 
        if ((tx == null) || (!tx.isOpen())) {
            this.tx = new BitsyTransaction(new BitsyTransactionContext(underlyingGraph.getStore()), getDefaultIsolationLevel(), underlyingGraph);
        }

        return tx; 
    }

    @Override
    /** This method can be used to check if the current threaded-graph is actively executing a transaction */
    public boolean isTransactionActive() {
        return (tx != null);
    }

    public BitsyIsolationLevel getDefaultIsolationLevel() {
        return underlyingGraph.getDefaultIsolationLevel();
    }
    
    public void setDefaultIsolationLevel(BitsyIsolationLevel level) {
        underlyingGraph.setDefaultIsolationLevel(level);
    }
    
    public BitsyIsolationLevel getTxIsolationLevel() {
        return getTx().getIsolationLevel();
    }
    
    public void setTxIsolationLevel(BitsyIsolationLevel level) {
        getTx().setIsolationLevel(level);   
    }

    @Override
    public void shutdown() {
    	// As per Blueprints tests, shutdown() implies automatic commit
    	if (tx == null) {
    		// Nothing to do
    	} else {
    		try {
    			// Stop the old transaction if it exists
    			tx.commit();
    		} finally {
    			// Remove this transaction -- independent of success/failure
    			this.tx = null;
    		}
    	}

    	// Don't mess with the graph store -- this is only a ThreadedGraph, not the main one
    }

//    @Deprecated
//    public void stopTransaction(Conclusion conclusion) {
//        stopTx(conclusion == Conclusion.SUCCESS);
//    }
    
//    @Override
//    public void commit() {
//        tx.save(commit);
//    }
//
//    @Override
//    public void rollback() {
//        stopTx(false);
//    }
//    
//    public void stopTx(boolean commit) {
//        if (tx == null) {
//            // Nothing to do
//        } else {
//            try {
//                // Stop the old transaction if it exists
//                tx.save(commit);
//            } finally {
//                // Remove this transaction -- independent of success/failure
//                this.tx = null;
//            }
//        }
//    }
//
//    @Override
//    public TransactionalGraph startTransaction() {
//        throw new UnsupportedOperationException("Can not startTransaction on a threaded transaction graph");
//    }
//    
//    @Override
//    public void shutdown() {
//        // As per Blueprints tests, shutdown() implies automatic commit
//        stopTx(true);
//        
//        // Don't mess with the graph store -- this is only a ThreadedGraph, not the main one
//    }
}
