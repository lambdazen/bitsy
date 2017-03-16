package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.IEdge;
import com.lambdazen.bitsy.UUID;

public interface IEdgeRemover {
    public IEdge removeEdge(UUID id);
}
