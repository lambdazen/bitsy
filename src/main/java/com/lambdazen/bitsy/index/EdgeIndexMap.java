package com.lambdazen.bitsy.index;

import com.lambdazen.bitsy.store.EdgeBean;
import java.util.Iterator;

public class EdgeIndexMap extends BitsyIndexMap<EdgeBean, EdgeIndex> {
    public EdgeIndexMap() {
        super();
    }

    public void createKeyIndex(String key, Iterator<EdgeBean> iter) {
        EdgeIndex index = new EdgeIndex(key, iter);
        addKeyIndex(key, index);
    }
}
