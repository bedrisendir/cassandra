package org.apache.cassandra.db.commitlog.capi;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.research.capiblock.Chunk;

public class AsyncChunkManager extends ChunkManager {
	static final Logger logger = LoggerFactory.getLogger(CAPIFlashCommitLog.class);
	public AsyncChunkManager(int num_async) {
		logger.error("[AsyncChunkManager - Devices =  " + DEVICES.length + "," + num_async + "]");
		for (int i = 0; i < DEVICES.length; i++) {
			logger.error(DEVICES[i]);
		}
		openChunks(num_async);
	}

	public AsyncChunkManager() {
		this(DatabaseDescriptor.getCAPIFlashCommitLogNumberOfAsyncCallsPerChunk());
	}

	@Override
	public void write(long startOffset, int num_blocks, CheckSummedBuffer buf) {
		Chunk cur = getNextChunk();
			try {
				cur.writeBlock(startOffset, num_blocks, buf.getBuffer());
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
}
