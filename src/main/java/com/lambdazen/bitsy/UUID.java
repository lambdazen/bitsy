package com.lambdazen.bitsy;

import com.fasterxml.jackson.annotation.JsonIgnore;

/** This class captures a UUID and is modeled after java.util.UUID */
public class UUID implements Comparable<UUID> {
    // Java guarantees that
    // "Reads and writes are atomic for reference variables and for most primitive variables (all types except long and double)."
    // Therefore, mostSigBits and leastSigBits must not be changed during the lifetime of this object.
    // Also, these objects must not be accessible to other threads without a memory flush/fence/barrier
    // Typically this memory barrier is handled by the ConcurrentHashMap that holds the vertex/edge bean.
    private final long mostSigBits;
    private final long leastSigBits;
    
    public UUID(long msb, long lsb) {
        this.mostSigBits = msb;
        this.leastSigBits = lsb;
    }

    @JsonIgnore
    public long getMostSignificantBits() {
        return mostSigBits;
    }

    @JsonIgnore
    public long getLeastSignificantBits() {
        return leastSigBits;
    }

    public String toString() {
        return uuidRepr();
    }
    
    public String uuidRepr() {
        return new java.util.UUID(mostSigBits, leastSigBits).toString();
    }
    
    public static UUID fromString(String str) {
        java.util.UUID ans = java.util.UUID.fromString(str);
        
        return new UUID(ans.getMostSignificantBits(), ans.getLeastSignificantBits());
    }

    public static UUID randomUUID() {
        java.util.UUID ans = java.util.UUID.randomUUID();
        
        return new UUID(ans.getMostSignificantBits(), ans.getLeastSignificantBits());
    }
    
    public int compareTo(UUID other) {
        if (this.mostSigBits < other.mostSigBits) {
            return -1;
        } else if (this.mostSigBits > other.mostSigBits) {
            return 1;
        } else if (this.leastSigBits < other.leastSigBits) {
            return -1;
        } else if (this.leastSigBits > other.leastSigBits) {
            return 1;
        } else {
            return 0;
        }
    }
    

    @Override
    public int hashCode() {
        // Same as java.util.UUID
        long hilo = mostSigBits ^ leastSigBits;
        return ((int)(hilo >> 32)) ^ (int) hilo;
    }
    
    @Override
    public boolean equals(Object obj) {
    	if (obj == null) {
            return false;
        } else if (this == obj) {
            return true;
        } else {
            try {
                UUID other = (UUID)obj;
                return (mostSigBits == other.getMostSignificantBits()) && (leastSigBits == other.getLeastSignificantBits());
            } catch (ClassCastException e) {
                return false;
            }
        }
    }

    public static String toString(UUID obj) {
        return new java.util.UUID(obj.getMostSignificantBits(), obj.getLeastSignificantBits()).toString();
    }
}
