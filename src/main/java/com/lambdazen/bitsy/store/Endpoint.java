package com.lambdazen.bitsy.store;

import com.lambdazen.bitsy.UUID;
import java.io.Serializable;

/** This class represents an end-point of an edge */
public class Endpoint implements Serializable, Comparable<Endpoint> {
    private static final long serialVersionUID = 3826290623768933884L;

    String edgeLabel;
    UUID edgeId;

    transient boolean marker;

    public Endpoint(String edgeLabel, UUID edgeId) {
        this.edgeLabel = edgeLabel;
        this.edgeId = edgeId;
        this.marker = false;
    }

    public String toString() {
        return "Endpoint(label = " + edgeLabel + ", edgeId = " + edgeId + ", marker = " + marker + ")";
    }

    public void setMarker() {
        marker = true;
    }

    public boolean isMarker() {
        return marker;
    }

    public String getEdgeLabel() {
        return edgeLabel;
    }

    public UUID getEdgeId() {
        return edgeId;
    }

    public int compareTo(Endpoint other) {
        int ans = 0;

        // A null edgeLabel implies the 'first' edge for the given vertex
        if (edgeLabel == null) {
            if (isMarker()) {
                return -1;
            }

            if (other.getEdgeLabel() != null) {
                return -1;
            }
        }

        // A null on the other implies that the other edge is first
        if (other.getEdgeLabel() == null) {
            if (other.isMarker()) {
                return 1;
            }

            if (edgeLabel != null) {
                return 1;
            } else {
                // Continue down to edge ID comparison
            }
        } else {
            // Comes here if both edge labels are non null
            ans = edgeLabel.compareTo(other.getEdgeLabel());
            if (ans != 0) {
                return ans;
            }

            // Continue down to edge ID comparison
        }

        // Both vertex and edge labels match.
        if (edgeId == null) {
            assert isMarker();
            return -1;
        }

        if (other.getEdgeId() == null) {
            assert other.isMarker();
            return 1;
        }

        ans = edgeId.compareTo(other.getEdgeId());
        if (ans != 0) {
            return ans;
        }

        if (isMarker()) {
            return -1;
        } else if (other.isMarker()) {
            return 1;
        } else {
            return 0;
        }
    }

    public boolean equals(Object o) {
        if (o instanceof Endpoint) {
            return compareTo((Endpoint) o) == 0;
        } else {
            return false;
        }
    }

    /**
     * This method must only be called on marker endpoints, i.e., end-points
     * used for matching existing Endpoints in the B-Tree. It returns true if
     * the given Endpoint matches this marker
     */
    public boolean isMatch(Endpoint other) {
        assert isMarker() : "isMatch can only be used on marker end-points";

        // Same structure as compareTo, except that nulls 'match' IDs (not <)
        int ans = 0;

        // A null edgeLabel implies that all endpoints of that vertex must be
        // matched
        if (edgeLabel == null) {
            return true;
        }

        if (other.getEdgeLabel() == null) {
            // This is not a match because the existing Endpoint no label, but
            // the marker does
            return false;
        }

        ans = edgeLabel.compareTo(other.getEdgeLabel());
        if (ans != 0) {
            return false;
        }

        // A null edgeId implies the 'first' edge for the given vertex for the
        // given edgeLabel
        if (edgeId == null) {
            return true;
        }

        return (edgeId.compareTo(other.getEdgeId()) == 0);
    }
}
