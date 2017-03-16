package com.lambdazen.bitsy;

public enum BitsyErrorCodes {
    INTERNAL_ERROR {
        public String toString() {
            return "INTERNAL_ERROR: An internal error occurred";
        }
    },
    
    ACCESS_OUTSIDE_TX_SCOPE {
        public String toString() {
            return "ACCESS_OUTSIDE_TX_SCOPE: A vertex/edge was accessed outside the scope of the transaction that created it";
        }
    },
    
    ELEMENT_ALREADY_DELETED {
        public String toString() {
            return "ELEMENT_ALREADY_DELETED: The vertex/edge being accessed has already been deleted";
        }
    },
    
    ADDING_EDGE_TO_A_DELETED_VERTEX {
        public String toString() {
            return "ADDING_EDGE_TO_A_DELETED_VERTEX: A edge was added in a transaction to (Direction.IN) a vertex that was deleted in the same transaction";
        }
    },

    ADDING_EDGE_FROM_A_DELETED_VERTEX {
        public String toString() {
            return "ADDING_EDGE_FROM_A_DELETED_VERTEX: A edge was added in a transaction from (Direction.OUT) a vertex that was deleted in the same transaction";
        }
    },
    
    EXCEPTION_IN_FLUSH {
        public String toString() {
            return "EXCEPTION_IN_FLUSH: The given exception occurred in the different thread while flushing a double buffer";
        }
    },
    
    BAD_DB_PATH {
        public String toString() {
            return "BAD_DB_PATH: The given path to the database is not a directory";
        }
    },
    
    ERROR_INITIALIZING_DB_FILES {
        public String toString() {
            return "ERROR_INITIALIZING_DB_FILES: The given database file could not be opened or initialized";
        }
    },
    
    ERROR_READING_FROM_FILE {
        public String toString() {
            return "ERROR_READING_FROM_FILE: A fatal error occurred while reading from a file. The database may need to be restarted after investigating the root cause";
        } 
    }, 
    
    ERROR_WRITING_TO_FILE {
        public String toString() {
            return "ERROR_WRITING_TO_TX_FILE: A fatal error occurred while writing to a transaction file. The database may need to be restarted after investigating the root cause";
        }
    },
    
    TRANSACTION_INTERRUPTED {
        public String toString() {
            return "TRANSACTION_INTERRUPTED: The given InterruptedException occurred during a transaction";
        }
    },
    
    SERIALIZATION_ERROR {
        public String toString() {
            return "SERIALIZATION_ERROR: The given exception occurred while serializing a commit block using Jackson JSON API";
        }
    },
    
    ADDING_EDGE_WITH_VERTEX_FROM_ANOTHER_TX {
        public String toString() {
            return "ADDING_EDGE_WITH_VERTICES_FROM_ANOTHER_TX: An edge was added in a transaction with an endpoint vertex belonging to another transaction";
        }
    },
    
    REMOVING_VERTEX_FROM_ANOTHER_TX  {
        public String toString() {
            return "REMOVING_VERTEX_FROM_ANOTHER_TX: A vertex was removed in a transaction different from the one in which it was loaded";
        }
    },
    
    REMOVING_EDGE_FROM_ANOTHER_TX {
        public String toString() {
            return "REMOVING_EDGE_FROM_ANOTHER_TX: An edge was removed in a transaction different from the one in which it was loaded";
        }
    },
    
    CHECKSUM_MISMATCH {
        public String toString() {
            return "CHECKSUM_MISMATCH: Encountered a checksum mismatch while loading a database file";
        }
    },
    
    CONCURRENT_MODIFICATION {
        public String toString() {
            return "CONCURRENT_MODIFICATION: A vertex or edge loaded in a transaction was concurrently modified by another transaction. Please retry this transaction";
        }
    },
    
    ERROR_IN_FILE_HEADER {
        public String toString() {
            return "ERROR_IN_FILE_HEADER: A fatal error occured while parsing the first line (header) in a database file";
        }
    },
    
    INCOMPLETE_TX_FLUSH {
        public String toString() {
            return "INCOMPLETE_TX_FLUSH: A V/E file was not fully flushed when the database was previously shutdown";
        }
    },
    
    DATABASE_IS_CORRUPT  {
        public String toString() {
            return "DATABASE_IS_CORRUPT: The given error is fatal and the database can not be loaded";
        }
    }, 
    
    JAVA_DESERIALIZATION_ERROR {
        public String toString() {
            return "DESERIALIZATION_ERROR: An error occurred while de-serializing a Java Serializable object";
        }
    },
    
    INDEX_ALREADY_EXISTS {
        public String toString() {
            return "INDEX_ALREADY_EXISTS: The given index already exists in the database";
        }
    },
    
    UNSUPPORTED_INDEX_TYPE {
        public String toString() {
            return "UNSUPPORTED_INDEX_TYPE: Indexes are only supported on Vertex and Edge classes";
        }
    },
    
    MISSING_INDEX {
        public String toString() {
            return "MISSING_INDEX: A vertex/edge query on a matching key-value pair was performed without creating an index for that key";
        }
    },
    
    OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS {
        public String toString() {
            return "OPERATION_UNDEFINED_FOR_NON_PERSISTENT_GRAPHS: The requested operation can not be performed on memory-only graphs";
        }
    },
    
    FULL_GRAPH_SCANS_ARE_DISABLED  {
        public String toString() {
            return "FULL_GRAPH_SCANS_ARE_DISABLED: The requested operation can not be performed because the BitsyGraph was constructed with the allowFullGraphScans option disabled";
        }
    },
    
    BAD_BACKUP_PATH { 
        public String toString() {
            return "BAD_BACKUP_PATH: The given path to backup the database is not an empty directory";
        }
    },
    
    BACKUP_IN_PROGRESS {
        public String toString() {
            return "BACKUP_IN_PROGRESS: A scheduled backup task is already in progress. Please try after a some time";
        }
    },
    
    BACKUP_INTERRUPTED {
        public String toString() {
            return "BACKUP_INTERRUPTED: The given InterruptedException occurred while waiting for a backup to be performed";
        }
    },
    
    BACKUP_FAILED {
        public String toString() {
            return "BACKUP_FAILED: The given exception occurred during a backup operation";
        }
    },
    
    FLUSH_INTERRUPTED  {
        public String toString() {
            return "FLUSH_INTERRUPTED: The given InterruptedException occurrend while waiting for a flush operation on a transaction log to complete";
        }
    },
    
    INSTANCE_ALREADY_EXISTS {
        public String toString() {
            return "INSTANCE_ALREADY_EXISTS: A BitsyGraph object with the same path has been registered with the MBeanServer. Creating multiple instances of BitsyGraph (without calling shutdown) will cause data corruption";
        }
    },
    
    ERROR_REGISTERING_TO_MBEAN_SERVER {
        public String toString() {
            return "ERROR_REGISTERING_TO_MBEAN_SERVER: A BitsyGraph object could not be registered with the MBeanServer";
        }
    },
    
    MAJOR_VERSION_MISMATCH {
        public String toString() {
            return "MAJOR_VERSION_MISMATCH: The database loaded was created with by different major version of Bitsy. Please run 'java com.lambdazen.bitsy.PortDatabase' to upgrade or downgrade the database";
        }
    },
    
    NO_OLAP_SUPPORT {
        public String toString() {
            return "NO_OLAP_SUPPORT: Bitsy is not designed to be an OLAP graph";
        }
    },
    NO_MULTI_PROPERTY_SUPPORT {
        public String toString() {
            return "NO_MULTI_PROPERTY_SUPPORT: Bitsy does not support multi-properties. Please use a list value instead";
        }
    },
    NO_META_PROPERTY_SUPPORT {
        public String toString() {
            return "NO_META_PROPERTY_SUPPORT: Bitsy does not support meta-properties. Please use property keys such as a.b.c instead";
        }
    },

    NO_CUSTOM_ID_SUPPORT {
    	public String toString() {
            return "NO_CUSTOM_ID_SUPPORT: Bitsy does not support user-supplied IDs for vertices and edges, only auto-generated UUIDs. Please move this to an indexed key";
        }
    }
}
