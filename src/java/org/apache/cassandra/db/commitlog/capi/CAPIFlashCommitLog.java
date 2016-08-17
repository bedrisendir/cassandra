/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog.capi;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.CAPIFlashCommitlogChunkManagerType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;

public class CAPIFlashCommitLog implements ICommitLog {
	static long START_OFFSET = DatabaseDescriptor.getCAPIFlashCommitLogStartOffset();
	static String[] DEVICES = DatabaseDescriptor.getCAPIFlashCommitLogDevices();
	static long DATA_OFFSET = START_OFFSET + FlashSegmentManager.MAX_SEGMENTS;
	static final Logger logger = LoggerFactory.getLogger(CAPIFlashCommitLog.class);
	public static final CAPIFlashCommitLog instance = new CAPIFlashCommitLog();
	final CapiBlockDevice dev = CapiBlockDevice.getInstance();
	FlashSegmentManager fsm;
	public  ChunkManager chunkManager;
	public  BufferAllocationStrategy bufferAlloc;
	
	protected CAPIFlashCommitLog() {
		try {
			logger.error("--------->"
					+ "Setting up capiflashcommitlog");
			fsm = new FlashSegmentManager(CapiBlockDevice.getInstance().openChunk(DEVICES[0]));
			CAPIFlashCommitlogChunkManagerType cmType = DatabaseDescriptor.getCAPIFlashCommitLogChunkManager();
			if (cmType == Config.CAPIFlashCommitlogChunkManagerType.AsyncProducerConsumerChunkManager) {
				chunkManager = new AsyncProducerConsumerChunkManager();
			} else if (cmType == Config.CAPIFlashCommitlogChunkManagerType.AsyncSemaphoreChunkManager) {
				chunkManager = new AsyncSemaphoreChunkManager();
			} else {
				chunkManager = new AsyncChunkManager();
			}
			bufferAlloc = DatabaseDescriptor
					.getCAPIFlashCommitLogBufferAllocationStrategy() == Config.CAPIFlashCommitlogBufferAllocationStrategyType.PooledAllocationStrategy
							? new PooledAllocationStrategy() : new FixedSizeAllocationStrategy();

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Appends row mutation to CommitLog Called from
	 * org.apache.cassandra.db.Keyspace.java Line 348 adds each mutation to
	 * Commitlog
	 * 
	 * @param
	 */
	public CommitLogPosition add(Mutation rm) {
		//test1.incrementAndGet();
		//test2.incrementAndGet();
		//logger.error(fsm.freelist.size()+"----------"+rm.getKeyspaceName());
		assert rm != null;
		long totalSize = Mutation.serializer.serializedSize(rm, MessagingService.current_version) + 28;
		int requiredBlocks = getBlockCount(totalSize);
		if (requiredBlocks > DatabaseDescriptor.getCAPIFlashCommitLogSegmentSizeInBlocks()) {
			// || requiredBlocks >
			// DatabaseDescriptor.getFlashCommitLogThreadBufferSizeinMB() *
			// (256)) {
			throw new IllegalArgumentException(
					String.format("Mutation of %s blocks is too large for the maxiumum size of %s", totalSize,
							DatabaseDescriptor.getCAPIFlashCommitLogSegmentSizeInBlocks()));
		}
		FlashRecordAdder adder = null;
		adder = fsm.allocate(requiredBlocks, rm);
		if (adder == null) {
			return null;
		}
		CheckSummedBuffer buf = null;
		buf = bufferAlloc.poll(requiredBlocks);
		// fill the buffer
		try {
			buf.getStream().writeLong(adder.getSegmentID());
			buf.getStream().writeInt((int) totalSize);
			buf.getBuffer().putLong(buf.calculateCRC(0, 12).getValue()); // TODO
			Mutation.serializer.serialize(rm, buf.getStream(), MessagingService.current_version);
			buf.getBuffer().putLong(buf.calculateCRC(20, ((int) totalSize) - 28).getValue()); // TODO
		} catch (IOException e) {
			e.printStackTrace();
		}
		chunkManager.write(adder.getStartBlock(), adder.getRequiredBlocks(), buf);
		bufferAlloc.free(buf);
		return new CommitLogPosition(adder.getSegmentID(), adder.getOffset());
	}

	/**
	 * Modifies the per-CF dirty cursors of any commit log segments for the
	 * column family according to the position given. Recycles it if it is
	 * unused. Called from org.apache.cassandra.db.ColumnFamilyStore.java at the
	 * end of flush operation
	 * 
	 * @param cfId
	 *            the column family ID that was flushed
	 * @param context
	 *            the replay position of the flush
	 */
	public void discardCompletedSegments(UUID cfId, final CommitLogPosition context) {
		// Go thru the active segment files, which are ordered oldest to
		// newest, marking the
		// flushed CF as clean, until we reach the segment file
		// containing the ReplayPosition passed
		// in the arguments. Any segments that become unused after they
		// are marked clean will be
		// recycled or discarded
		logger.error("discard completed log segments for {}, column family {}", context, cfId);
		for (Iterator<FlashSegment> iter = fsm.getActiveSegments().iterator(); iter.hasNext();) {
			FlashSegment segment = iter.next();
			segment.markClean(cfId, context);
			// If the segment is no longer needed, and we have another
			// spare segment in the hopper
			// (to keep the last segment from getting discarded), pursue
			// either recycling or deleting
			// this segment file.
			if (iter.hasNext()) {
				if (segment.isUnused()) {
					logger.error("Commit log segment {} is unused ", segment.physical_block_address);
					fsm.recycleSegment(segment);
				} else {
					logger.error("Not safe to delete commit log segment {}; dirty is {} ",
							segment.physical_block_address, segment.dirtyString());
				}
			} else {
				logger.error("Not deleting active commitlog segment {} ", segment.physical_block_address);
			}
			if (segment.contains(context)) {
				logger.error("Segment " + segment.id + " contains the context");
				break;
			}
		}
		if (fsm.hasAvailableSegments.hasWaiters() && !fsm.freelist.isEmpty()) {
			fsm.hasAvailableSegments.signalAll();
		}
	}

	/**
	 * Recover
	 */
	public int recover() {
		//TODO Implement
		/*
		long startTime = System.currentTimeMillis();
		FlashBulkReplayer r = new FlashBulkReplayer();
		try {
			r.recover(fsm);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long count = r.blockForWrites();
		fsm.recycleAfterReplay();
		long estimatedTime = System.currentTimeMillis() - startTime;
		logger.debug("------------------------>" + " Replayed " + count + " records in " + estimatedTime);
		return (int) count;*/
		return 0;
		
	}

	/**
	 * Shuts down the threads used by the commit log, blocking until completion.
	 */
	public void shutdownBlocking() {
		try {
			chunkManager.closeChunks();
			fsm.bookkeeper.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public CommitLogPosition getCurrentPosition() {
		return fsm.active.getContext();
	}

	static int getBlockCount(long size) {
		return (int) (Math.ceil((double) size / (CapiBlockDevice.BLOCK_SIZE)));
	}


	public void forceRecycleAllSegments() {
		forceRecycleAllSegments(Collections.<UUID> emptyList());
	}

	public void forceRecycleAllSegments(Iterable<UUID> droppedCfs) {
		fsm.forceRecycleAll(droppedCfs);
	}

	@Override
	public void await() {
		fsm.hasAvailableSegments.register(Metrics
				.timer(new DefaultNameFactory("CommitLog").createMetricName("WaitingOnSegmentAllocation")).time())
				.awaitUninterruptibly();
	}
}
