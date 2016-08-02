package org.apache.cassandra.db.commitlog.capi;

public interface BufferAllocationStrategy {
	void free(CheckSummedBuffer buf);
	CheckSummedBuffer poll(long requiredBlocks);
}
