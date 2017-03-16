package com.lambdazen.bitsy.index;

import java.util.Iterator;

import com.lambdazen.bitsy.store.VertexBean;

public class VertexIndexMap extends BitsyIndexMap<VertexBean, VertexIndex> {
    public VertexIndexMap() {
        super();
    }

    public void createKeyIndex(String key, Iterator<VertexBean> iter) {
        VertexIndex index = new VertexIndex(key, iter);
        addKeyIndex(key, index);
    }
}
