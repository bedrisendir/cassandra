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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bsendir
 *
 */
public class FlashSegment {
	static final Logger logger = LoggerFactory.getLogger(FlashSegment.class);
	// cache
	public final Map<UUID, Integer> cfLastWrite = new HashMap<>();

	// create a unique id per segment
	private final static long idBase = System.currentTimeMillis();
	private final static AtomicInteger nextId = new AtomicInteger(1);

	// unique id of this segment
	final long id;
	final int physical_block_address;// bookkeeping address

	public AtomicLong currentBlocks = new AtomicLong(0);

	public static long getNextId() {
		return idBase + nextId.getAndIncrement();
	}

	public FlashSegment(int pb_id) {
		id = getNextId();
		physical_block_address = pb_id;
	}

	public synchronized boolean isActive() {
		return !cfLastWrite.isEmpty();
	}

	public boolean hasCapacityFor(long blocks) {
		return blocks <= (FlashSegmentManager.BLOCKS_IN_SEG - currentBlocks
				.get());
	}

	public CommitLogPosition getContext() {
		return new CommitLogPosition(id, (int) currentBlocks.get());
	}

	public long getID() {
		return id;
	}

	public long getPB() {
		return physical_block_address;
	}

	
	public long getandAddPosition(long blocks) {
		return (long) CAPIFlashCommitLog.DATA_OFFSET
				+ ((long) (physical_block_address) * FlashSegmentManager.BLOCKS_IN_SEG)
				+ (long) currentBlocks.getAndAdd(blocks);
	}

	static long calculatePos(long start, long segmentno) {
		return CAPIFlashCommitLog.DATA_OFFSET
				+ (long) ((segmentno) * FlashSegmentManager.BLOCKS_IN_SEG)
				+ start;
	}

	 /**
     * mark all of the column families we're modifying as dirty at this position
     */
	public void markDirty(Mutation rm, CommitLogPosition repPos) {
		for (PartitionUpdate columnFamily : rm.getPartitionUpdates()) {
			// check for null cfm in case a cl write goes through after the cf
			// is
			// defined but before a new segment is created.
			CFMetaData cfm = Schema.instance.getCFMetaData(columnFamily.metadata().cfId);
			if (cfm == null) {
				logger.debug("Attempted to write commit log entry for unrecognized column family: "
						+ columnFamily.metadata().cfId);
			} else {
				markCFDirty(columnFamily.metadata().cfId, repPos.position);
			}
		}
	}

	private void markCFDirty(UUID cfId, int position) {
		cfLastWrite.put(cfId, position);
	}

	public synchronized Collection<UUID> getDirtyCFIDs() {
		return new ArrayList<>(cfLastWrite.keySet());
	}

	public synchronized void markClean(UUID cfId, CommitLogPosition context) {
		Integer lastWritten = cfLastWrite.get(cfId);
		if (lastWritten != null
				&& (!contains(context) || lastWritten < context.position)) {
			cfLastWrite.remove(cfId);
		}
	}

	public synchronized boolean isUnused() {
		return cfLastWrite.isEmpty();
	}

	public boolean contains(CommitLogPosition context) {
		return context.segmentId == id;
	}
	
	/**
	 * Used only for debugging
	 * @return
	 */
	public String dirtyString() {
		StringBuilder sb = new StringBuilder();
		for (UUID cfId : getDirtyCFIDs()) {
			CFMetaData m = Schema.instance.getCFMetaData(cfId);
			sb.append(m == null ? "<deleted>" : m.cfName).append(" (")
					.append(cfId).append("), ");
		}
		return sb.toString();
	}
}
