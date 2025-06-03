package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.IEdge;
import com.lambdazen.bitsy.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.tinkerpop.gremlin.structure.Direction;

/** This map is used to keep track of edges between vertices in the transaction context. */
public class AdjacencyMap {
    Map<UUID, TreeMap<Endpoint, Integer>> outV;
    Map<UUID, TreeMap<Endpoint, Integer>> inV;
    IEdgeRemover edgeRemover;

    public AdjacencyMap(boolean isConcurrent, IEdgeRemover edgeRemover) {
        this.edgeRemover = edgeRemover;
        this.outV = new HashMap<UUID, TreeMap<Endpoint, Integer>>();
        this.inV = new HashMap<UUID, TreeMap<Endpoint, Integer>>();
    }

    public void clear() {
        outV.clear();
        inV.clear();
    }

    public void addEdge(UUID edgeId, UUID outVId, String label, UUID inVId, int version) {
        // Update in and out vertices
        addToTreeMap(outV, outVId, new Endpoint(label, edgeId), version);
        addToTreeMap(inV, inVId, new Endpoint(label, edgeId), version);
    }

    private void addToTreeMap(Map<UUID, TreeMap<Endpoint, Integer>> adjMap, UUID id, Endpoint endpoint, int version) {
        TreeMap<Endpoint, Integer> value = adjMap.get(id);
        if (value == null) {
            value = new TreeMap<Endpoint, Integer>();
            adjMap.put(id, value);
        }

        value.put(endpoint, version);
    }

    public void removeEdge(UUID edgeId, UUID outVId, String label, UUID inVId) {
        // If the combination of given parameters don't match an existing edge, it won't be removed
        if (removeMatchingKeys(outV.get(outVId), new Endpoint(label, edgeId), null, null)) {
            outV.remove(outVId);
        }

        if (removeMatchingKeys(inV.get(inVId), new Endpoint(label, edgeId), null, null)) {
            inV.remove(inVId);
        }
    }

    public void removeVertex(UUID vertexId) {
        // Remove incoming and outgoing vertices
        if (removeMatchingKeys(outV.get(vertexId), new Endpoint(null, null), inV, Direction.OUT)) {
            outV.remove(vertexId);
        }

        if (removeMatchingKeys(inV.get(vertexId), new Endpoint(null, null), outV, Direction.IN)) {
            inV.remove(vertexId);
        }
    }

    public List<UUID> getEdges(UUID vertexId, Direction dir, String[] edgeLabels) {
        NavigableMap<Endpoint, Integer> map = (dir == Direction.IN) ? inV.get(vertexId) : outV.get(vertexId);

        if (map == null) {
            return Collections.emptyList();
        }

        Endpoint[] matches;

        // Var-args appear as empty arrays
        if ((edgeLabels == null) || edgeLabels.length == 0) {
            matches = new Endpoint[] {new Endpoint(null, null)};
        } else {
            int len = edgeLabels.length;
            matches = new Endpoint[len];
            for (int i = 0; i < len; i++) {
                matches[i] = new Endpoint(edgeLabels[i], null);
            }
        }

        List<UUID> edgeIds = new ArrayList<UUID>();

        for (Endpoint match : matches) {
            findMatchingValues(map, match, edgeIds);
        }

        return edgeIds;
    }

    // This method removes all keys that match the given value
    private boolean removeMatchingKeys(
            NavigableMap<Endpoint, Integer> map,
            Endpoint endpoint,
            Map<UUID, TreeMap<Endpoint, Integer>> otherMap,
            Direction dir) {
        if (map == null) {
            return false;
        }

        // Mark this endpoint as a marker, because it is used to do a tailMap traversal to remove matching edges
        endpoint.setMarker();

        // Find the first key
        Endpoint floorKey = map.floorKey(endpoint);

        Map<Endpoint, Integer> view;
        if (floorKey == null) {
            // This means that the element being searched is the minimum
            view = map;
        } else {
            view = map.tailMap(floorKey);
        }

        Iterator<Map.Entry<Endpoint, Integer>> entryIter = view.entrySet().iterator();

        boolean isFirst = true;
        while (entryIter.hasNext()) {
            Map.Entry<Endpoint, Integer> entry = entryIter.next();
            Endpoint key = entry.getKey();

            if (endpoint.isMatch(key)) {
                // Remove it from this index
                entryIter.remove();

                // and from the underlying edge map if necessary
                if (dir != null) { // Direction is null if this is a recursive all
                    // Remove the edge if the map is provided for this purpose
                    UUID edgeId = key.getEdgeId();

                    IEdge edge = edgeRemover.removeEdge(edgeId);

                    assert (edge != null);

                    // Remove the other endpoint of this edge. NOTE: Self loops are not allowed.
                    Endpoint otherEndpoint;
                    UUID otherVertexId;
                    if (dir == Direction.OUT) {
                        otherVertexId = edge.getInVertexId();
                        otherEndpoint = new Endpoint(key.getEdgeLabel(), key.getEdgeId());
                    } else {
                        otherVertexId = edge.getOutVertexId();
                        otherEndpoint = new Endpoint(key.getEdgeLabel(), key.getEdgeId());
                    }

                    if (removeMatchingKeys(otherMap.get(otherVertexId), otherEndpoint, null, null)) {
                        otherMap.remove(otherVertexId);
                    }
                }
            } else {
                // Done with removes -- the tree map is sorted
                if (isFirst) {
                    // continue
                } else {
                    break;
                }
            }

            isFirst = false;
        }

        return (map.size() == 0);
    }

    // This method removes all keys that match the given value
    private void findMatchingValues(NavigableMap<Endpoint, Integer> map, Endpoint endpoint, List<UUID> result) {
        // Mark this endpoint as a marker, because it is used to do a tailMap traversal to query matching edges
        endpoint.setMarker();

        // Find the first key
        Endpoint floorKey = map.floorKey(endpoint);

        Map<Endpoint, Integer> view;
        if (floorKey == null) {
            // This means that the element being searched is the minimum
            view = map;
        } else {
            view = map.tailMap(floorKey);
        }

        boolean isFirst = true;
        for (Map.Entry<Endpoint, Integer> entry : view.entrySet()) {
            Endpoint key = entry.getKey();

            if (endpoint.isMatch(key)) {
                // Matching entry, must be added to result
                result.add(key.getEdgeId());
            } else {
                // Done with the search -- the tree map is sorted

                if (isFirst) {
                    // continue
                } else {
                    break;
                }
            }

            isFirst = false;
        }
    }
}
