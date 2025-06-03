package com.lambdazen.bitsy.index;

import com.lambdazen.bitsy.BitsyElement;
import com.lambdazen.bitsy.BitsyState;
import com.lambdazen.bitsy.store.EdgeBean;
import com.lambdazen.bitsy.store.VertexBean;
import java.util.ArrayList;
import java.util.Collection;

public class IndexHelper {
    public static <T extends BitsyElement> Collection<T> filterElementsByKeyValue(
            Collection<T> elems, String key, Object value) {
        ArrayList<T> ans = new ArrayList<T>();

        for (T elem : elems) {
            if (elem.getState() == BitsyState.D) {
                continue;
            }

            Object elemVal = elem.value(key);

            if ((elemVal != null) && (elemVal.equals(value))) {
                ans.add(elem);
            }
        }

        return ans;
    }

    public static Collection<VertexBean> filterVertexBeansByKeyValue(
            Collection<VertexBean> elems, String key, Object value) {
        ArrayList<VertexBean> ans = new ArrayList<VertexBean>();

        for (VertexBean elem : elems) {
            Object elemVal = (elem.getProperties() == null)
                    ? null
                    : (elem.getProperties().get(key));

            if ((elemVal != null) && (elemVal.equals(value))) {
                ans.add(elem);
            }
        }

        return ans;
    }

    public static Collection<EdgeBean> filterEdgeBeansByKeyValue(Collection<EdgeBean> elems, String key, Object value) {
        ArrayList<EdgeBean> ans = new ArrayList<EdgeBean>();

        for (EdgeBean elem : elems) {
            Object elemVal = (elem.getPropertiesDict() == null)
                    ? null
                    : (elem.getPropertiesDict().getProperty(key));

            if ((elemVal != null) && (elemVal.equals(value))) {
                ans.add(elem);
            }
        }

        return ans;
    }
}
