package com.lambdazen.bitsy.util;

import java.util.Collection;
import java.util.Iterator;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.tx.BitsyTransaction;

public class EdgeIterator extends BitsyElementIterator<EdgeBean, BitsyEdge> implements Iterator<BitsyEdge> {
    private BitsyTransaction tx;
    
    public EdgeIterator(BitsyTransaction tx, Collection<BitsyEdge> txEdges, Collection<EdgeBean> vertices) {
        super(vertices, txEdges.iterator());
        
        this.tx = tx;
    }

    public EdgeIterator(BitsyTransaction tx, Collection<BitsyEdge> txEdges, Collection<EdgeBean> vertices, Collection<BitsyEdge> allTxEdges) {
        super(vertices, txEdges.iterator(), allTxEdges);
        
        this.tx = tx;
    }

    @Override
    public UUID getId(EdgeBean bean) {
        return bean.getId();
    }
    
    @Override
    public BitsyEdge getElement(EdgeBean bean) {
        return (BitsyEdge)tx.getEdge(bean.getId());
    }
}
