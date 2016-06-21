package org.apache.cassandra.db.commitlog.capi;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.ReplayPosition;

public interface ICommitLog {

	ReplayPosition getContext();

	ReplayPosition add(Mutation mutation);

	int recover() throws IOException;

	void shutdownBlocking() throws InterruptedException;

	void discardCompletedSegments(UUID cfId, ReplayPosition x);

	void forceRecycleAllSegments(Iterable<UUID> droppedCfs);

	void await();

	void forceRecycleAllSegments();

}
