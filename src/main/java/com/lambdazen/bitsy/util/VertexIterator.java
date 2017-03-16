package com.lambdazen.bitsy.util;

import java.util.Collection;
import java.util.Iterator;

import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.UUID;
import com.lambdazen.bitsy.store.VertexBean;
import com.lambdazen.bitsy.tx.BitsyTransaction;

public class VertexIterator extends BitsyElementIterator<VertexBean, BitsyVertex> implements Iterator<BitsyVertex> {
    private BitsyTransaction tx;
    
    public VertexIterator(BitsyTransaction tx, Collection<BitsyVertex> txVertices, Collection<VertexBean> vertices) {
        super(vertices, txVertices.iterator());
        
        this.tx = tx;
    }

    public VertexIterator(BitsyTransaction tx, Collection<BitsyVertex> txVertices, Collection<VertexBean> vertices, Collection<BitsyVertex> allChangedVertices) {
        super(vertices, txVertices.iterator(), allChangedVertices);
        
        this.tx = tx;
    }

    @Override
    public UUID getId(VertexBean bean) {
        return bean.getId();
    }
    
    @Override
    public BitsyVertex getElement(VertexBean bean) {
        return (BitsyVertex)tx.getVertex(bean.getId());
    }
}
