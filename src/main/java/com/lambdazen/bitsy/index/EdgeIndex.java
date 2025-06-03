package com.lambdazen.bitsy.index;

import com.lambdazen.bitsy.ads.dict.Dictionary;
import com.lambdazen.bitsy.store.EdgeBean;
import java.util.Iterator;

public class EdgeIndex extends BitsyIndex<EdgeBean> {
    String key;

    public EdgeIndex(String key, Iterator<EdgeBean> initialContents) {
        super();

        this.key = key;

        load(initialContents);
    }

    @Override
    public Object getValue(EdgeBean bean) {
        Dictionary props = bean.getPropertiesDict();
        return (props == null) ? null : props.getProperty(key);
    }

    @Override
    public EdgeBean copy(EdgeBean bean) {
        return new EdgeBean(bean);
    }
}
