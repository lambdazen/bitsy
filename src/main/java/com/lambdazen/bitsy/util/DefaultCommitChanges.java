package com.lambdazen.bitsy.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.lambdazen.bitsy.BitsyEdge;
import com.lambdazen.bitsy.BitsyException;
import com.lambdazen.bitsy.BitsyVertex;
import com.lambdazen.bitsy.ICommitChanges;

public class DefaultCommitChanges implements ICommitChanges {
    private List<BitsyVertex> changedVertices;
    private List<BitsyEdge> changedEdges;

    public DefaultCommitChanges() {
        this.changedVertices = new ArrayList<BitsyVertex>();
        this.changedEdges = new ArrayList<BitsyEdge>();
    }
    
    @Override
    public Collection<BitsyVertex> getVertexChanges() {
        return changedVertices;
    }

    @Override
    public Collection<BitsyEdge> getEdgeChanges() {
        return changedEdges;
    }
    
    public void reset() {
        // Clear retains the size from previous round, which is useful to load V/E logs
        changedVertices.clear();
        changedEdges.clear();
    }
    
    public void changeVertex(BitsyVertex vertex) throws BitsyException {
        changedVertices.add(vertex);
    }

    public void changeEdge(BitsyEdge edge) throws BitsyException {
        changedEdges.add(edge);
    }
}
