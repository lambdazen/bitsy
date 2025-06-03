package com.lambdazen.bitsy;

import java.util.Collection;

public interface ICommitChanges {
    public Collection<BitsyVertex> getVertexChanges();

    public Collection<BitsyEdge> getEdgeChanges();
}
