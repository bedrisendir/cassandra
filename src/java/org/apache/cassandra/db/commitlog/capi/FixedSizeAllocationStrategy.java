package org.apache.cassandra.db.commitlog.capi;

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedSizeAllocationStrategy implements BufferAllocationStrategy {
	static final Logger logger = LoggerFactory.getLogger(FixedSizeAllocationStrategy.class);
	ConcurrentLinkedQueue<CheckSummedBuffer> buffers = new ConcurrentLinkedQueue<CheckSummedBuffer>();

	public FixedSizeAllocationStrategy() {
		for (int i = 0; i < DatabaseDescriptor.getCAPIFlashCommitlogNumberOfBuffers(); i++) {
			buffers.add(new CheckSummedBuffer());
		}
	}

	@Override
	public CheckSummedBuffer poll(long requiredBlocks) {
		CheckSummedBuffer ret = null;
		// busy wait if resource if not available
		while ((ret = buffers.poll()) == null)
			;
		return ret;
	}

	@Override
	public void free(CheckSummedBuffer buf) {
		buf.clear();
		buffers.add(buf);
	}
}
