package com.lambdazen.bitsy.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;

import com.lambdazen.bitsy.ads.set.ClassifierGetter;
import com.lambdazen.bitsy.ads.set.CompactMultiSetMax;
import com.lambdazen.bitsy.ads.set.CompactSet;
import com.lambdazen.bitsy.ads.set.Set24;
import com.lambdazen.bitsy.ads.set.SetMax;

/** This map is used to keep track of edges between vertices in the memory graph store. */ 
public class AdjacencyMapForBeans {
    IEdgeRemover edgeRemover;
    ClassifierGetter<String, EdgeBean> edgeBeanClassifier;
    
    public AdjacencyMapForBeans(boolean isConcurrent, IEdgeRemover edgeRemover) {
        this.edgeRemover = edgeRemover;
        this.edgeBeanClassifier = new ClassifierGetter<String, EdgeBean>() {
            @Override
            public String getClassifier(EdgeBean eBean) {
                return eBean.getLabel();
            }
        };
    }

    public void addEdge(EdgeBean eBean) {
        // Update in and out vertices
        VertexBean outV = eBean.outVertex;
        VertexBean inV = eBean.inVertex;
        
        outV.outEdges = addEdgeToAdjList(outV.outEdges, eBean);
        inV.inEdges = addEdgeToAdjList(inV.inEdges, eBean);
    }

    private Object addEdgeToAdjList(Object adjList, EdgeBean eBean) {
        if (adjList instanceof CompactMultiSetMax) {
            CompactMultiSetMax<String, EdgeBean> adjListMultiSet = (CompactMultiSetMax<String, EdgeBean>)adjList;
            
            return adjListMultiSet.add(eBean, edgeBeanClassifier); 
        } else {
            Object ans = CompactSet.<EdgeBean>add(adjList, eBean);
            
            if (ans instanceof SetMax) {
                return convertToMultiSet((SetMax)ans);
            } else {
                return ans;
            }
        }
    }

    /** This method removes the edges without calling edgeRemover.removeEdge() */
    protected void removeEdgeWithoutCallback(EdgeBean eBean) {
        if (eBean == null) {
            return;
        }

        // Update in and out vertices
        VertexBean outV = eBean.outVertex;
        VertexBean inV = eBean.inVertex;

        outV.outEdges = removeEdgeFromAdjList(outV.outEdges, eBean); // CompactSet.<EdgeBean>remove(outV.outEdges, eBean);
        inV.inEdges = removeEdgeFromAdjList(inV.inEdges, eBean);; // CompactSet.<EdgeBean>remove(inV.inEdges, eBean);
    }
    
    private Object removeEdgeFromAdjList(Object adjList, EdgeBean eBean) {
        if (adjList instanceof CompactMultiSetMax) {
            CompactMultiSetMax<String, EdgeBean> adjListMultiSet = (CompactMultiSetMax<String, EdgeBean>)adjList;
            
            CompactMultiSetMax<String, EdgeBean> ans = adjListMultiSet.remove(eBean, edgeBeanClassifier);
            
            // The first check is fast and increases the chances of hitting the second one 
            if ((ans.getOccupiedCells() <= 13) && !ans.sizeBiggerThan24()) {
                return convertToSet(ans);
            } else {
                return ans;
            }
        } else {
            return CompactSet.<EdgeBean>remove(adjList, eBean);
        }
    }
    
    private Object[] getEdgesFromAdjList(Object adjList) {
        if (adjList instanceof CompactMultiSetMax) {
            CompactMultiSetMax<String, EdgeBean> adjListMultiSet = (CompactMultiSetMax<String, EdgeBean>)adjList;
            
            return adjListMultiSet.getAllElements();
        } else {
            return CompactSet.getElements(adjList);
        }
    }

    private Object convertToSet(CompactMultiSetMax<String, EdgeBean> set) {
        // The size is guaranteed to be between 13 and 24
        // The Set24 will automatically adjust on a remove if it is too big
        return new Set24<EdgeBean>(set.getAllElements());
    }

    private Object convertToMultiSet(SetMax<EdgeBean> set) {
        // Use safe = false, which means that the multi-set can use the default SetMax implementation
        CompactMultiSetMax<String, EdgeBean> ans = new CompactMultiSetMax<String, EdgeBean>(36, false);
        
        for (Object obj : set.getElements()) {
            if (obj != null) {
                ans.add((EdgeBean)obj, edgeBeanClassifier);
            }
        }

        return ans;
    }

    public void removeVertex(VertexBean vBean) {
        if (vBean == null) {
            return;
        }

        // Remove outgoing edges
        for (Object obj : getEdgesFromAdjList(vBean.outEdges)) {
            EdgeBean eBean = (EdgeBean)obj;
            eBean.inVertex.inEdges = removeEdgeFromAdjList(eBean.inVertex.inEdges, eBean); // CompactSet.<EdgeBean>remove(eBean.inVertex.inEdges, eBean);

            // Callback
            edgeRemover.removeEdge(eBean.getId());
        }
        
        vBean.outEdges = null;

        // Remove incoming edges
        for (Object obj : getEdgesFromAdjList(vBean.inEdges)) {
            EdgeBean eBean = (EdgeBean)obj;
            eBean.outVertex.outEdges = removeEdgeFromAdjList(eBean.outVertex.outEdges, eBean); // CompactSet.<EdgeBean>remove(eBean.outVertex.outEdges, eBean);

            // Callback
            edgeRemover.removeEdge(eBean.getId());
        }

        vBean.inEdges = null;
    }
    
    public List<EdgeBean> getEdges(VertexBean vBean, Direction dir, String[] edgeLabels) {
        if (vBean == null) {
            return Collections.emptyList();
        }

        boolean outDirection = (dir == Direction.OUT);

        Object edges = outDirection ? vBean.outEdges : vBean.inEdges;
        if (edges == null) {
            return Collections.emptyList();
        }

        // Need a new list
        List<EdgeBean> ans = new ArrayList<EdgeBean>();

        boolean origMatch = (edgeLabels == null) || edgeLabels.length == 0;

        boolean multiSet = (edges instanceof CompactMultiSetMax<?, ?>); 
        String[] edgeLabelsToQuery = (multiSet && (!origMatch)) ? edgeLabels : new String[] {null};
        
        for (String edgeLabelToQuery : edgeLabelsToQuery) {
            Object[] edgeBeans;
            if (multiSet) {
                edgeBeans = ((CompactMultiSetMax<String, EdgeBean>)edges).getSuperSetWithClassifier(edgeLabelToQuery);
            } else {
                edgeBeans = CompactSet.getElements(edges);
            }
    
            for (Object obj : edgeBeans) {
                // Always check for nulls on getElements() because reads don't acquire locks
                if (obj == null) {
                    continue;
                }
    
                boolean match = origMatch;
    
                EdgeBean eBean = (EdgeBean)obj;

                if (!match) {
                    String eLabel = eBean.getLabel();
                    if (edgeLabelToQuery != null) {
                        match = eLabel.equals(edgeLabelToQuery);
                    } else {
                        for (String label : edgeLabels) {
                            if (eLabel.equals(label)) {
                                match = true;
                                break;
                            }
                        }
                    }
                }
    
                if (match) {
                    // Always copy objects that are directly picked up from the memory store
                    // These objects can be updated later while the query is in progress
                    ans.add(new EdgeBean(eBean));
                }
            }
        }

        return ans;
    }
}
