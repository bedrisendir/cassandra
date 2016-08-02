package org.apache.cassandra.db.commitlog.capi;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;

public interface ICommitLog {

	CommitLogPosition  getCurrentPosition();

	CommitLogPosition add(Mutation mutation);

	int recover() throws IOException;

	void shutdownBlocking() throws InterruptedException;

	void discardCompletedSegments(UUID cfId, CommitLogPosition x);

	void forceRecycleAllSegments(Iterable<UUID> droppedCfs);

	void await();

	void forceRecycleAllSegments();

}
