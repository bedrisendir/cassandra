package org.apache.cassandra.db.commitlog.capi;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.DatabaseDescriptor;
import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

abstract class ChunkManager {
	static String[] DEVICES = DatabaseDescriptor.getCAPIFlashCommitLogDevices();
	final CapiBlockDevice dev = CapiBlockDevice.getInstance();
	final Chunk chunks[] = new Chunk[DatabaseDescriptor.getCAPIFlashCommitLogNumberOfChunks()];
	final AtomicInteger nextChunk = new AtomicInteger(0);

	abstract void write(long l, int m, CheckSummedBuffer buf);

	protected void openChunks(int num_async) {
		for (int i = 0; i < chunks.length; i++) {
			try {
				if (num_async == 0) {
					// let the device decide max num of requests
					chunks[i] = dev.openChunk(DEVICES[i % DEVICES.length]);
				} else {
					// user defined max requests per chunk
					chunks[i] = dev.openChunk(DEVICES[i % DEVICES.length], num_async);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	protected void closeChunks() {
		for (int i = 0; i < chunks.length; i++) {
			try {
				chunks[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	protected Chunk getNextChunk() {
		return chunks[Math.abs(nextChunk.getAndIncrement() % chunks.length)];
	}
}
