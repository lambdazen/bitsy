package com.lambdazen.bitsy.index;

import com.lambdazen.bitsy.store.VertexBean;
import java.util.Iterator;
import java.util.Map;

public class VertexIndex extends BitsyIndex<VertexBean> {
    String key;

    public VertexIndex(String key, Iterator<VertexBean> initialContents) {
        super();

        this.key = key;

        load(initialContents);
    }

    @Override
    public Object getValue(VertexBean bean) {
        Map<String, Object> props = bean.getProperties();
        return (props == null) ? null : props.get(key);
    }

    @Override
    public VertexBean copy(VertexBean bean) {
        return new VertexBean(bean);
    }
}
