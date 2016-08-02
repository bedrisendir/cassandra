package org.apache.cassandra.db.commitlog.capi;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.Chunk;

public class AsyncProducerConsumerChunkManager extends ChunkManager {
	static final Logger logger = LoggerFactory.getLogger(AsyncProducerConsumerChunkManager.class);

	BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(
			DatabaseDescriptor.getCAPIFlashCommitlogNumberOfAsyncWrite());
	final ExecutorService service = new ThreadPoolExecutor(1,
			DatabaseDescriptor.getCAPIFlashCommitlogMaxNumberOfConsumers(), 1, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<Runnable>(DatabaseDescriptor.getCAPIFlashCommitlogNumberOfAsyncWrite()));

	public AsyncProducerConsumerChunkManager(int num_async) {
		logger.debug("[AsyncProducerConsumerChunkManager - Devices =  " + DEVICES.length + "," + num_async + "]");
		logger.debug("[AsyncProducerConsumerChunkManager - Concurrent Chunk Writers =  "
				+ DatabaseDescriptor.getCAPIFlashCommitlogNumberOfAsyncWrite() + "- Consumers="
				+ DatabaseDescriptor.getCAPIFlashCommitlogMaxNumberOfConsumers() + "]");
		openChunks(num_async);
		
		for (int i = 0; i < DatabaseDescriptor.getCAPIFlashCommitlogNumberOfAsyncWrite(); i++) {
			try {
				queue.put(new Task());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public AsyncProducerConsumerChunkManager() {
		this(DatabaseDescriptor.getCAPIFlashCommitLogNumberOfAsyncCallsPerChunk());
	}

	@Override
	public void write(long startOffset, int num_blocks, CheckSummedBuffer buf) {
		Chunk cur = getNextChunk();
		try {
			Task t = (Task) queue.take();
			t.future = cur.writeBlockAsync(startOffset, num_blocks, buf.getBuffer());
			service.submit(t).get();
			queue.put(t);
		} catch (IOException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	private class Task implements Runnable {
		Future<Long> future = null;
		@Override
		public void run() {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
}
