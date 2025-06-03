package com.lambdazen.bitsy.index;

import com.lambdazen.bitsy.store.VertexBean;
import java.util.Iterator;

public class VertexIndexMap extends BitsyIndexMap<VertexBean, VertexIndex> {
    public VertexIndexMap() {
        super();
    }

    public void createKeyIndex(String key, Iterator<VertexBean> iter) {
        VertexIndex index = new VertexIndex(key, iter);
        addKeyIndex(key, index);
    }
}
