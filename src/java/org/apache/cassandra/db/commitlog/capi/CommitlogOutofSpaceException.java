package org.apache.cassandra.db.commitlog.capi;

public class CommitlogOutofSpaceException extends Exception {
	
    public CommitlogOutofSpaceException(String message) {
        super(message);
    }
}