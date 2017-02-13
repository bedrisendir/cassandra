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
package org.apache.cassandra.db.commitlog.capi.storage;

import java.io.IOException;
import java.util.concurrent.Semaphore;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.capi.util.CheckSummedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.Chunk;

public class SyncChunkManager extends ChunkManager {
	static final Logger logger = LoggerFactory.getLogger(SyncChunkManager.class);
	private final Semaphore semaphore = new Semaphore(DatabaseDescriptor.getCAPIFlashCommitlogNumberOfAsyncWrite(), false);

	public SyncChunkManager(int num_async) {
		logger.error("[SyncChunkManager - Devices =  " + DEVICES.length + "," + num_async + "]");
		logger.error("[SyncChunkManager - Concurrent Chunk Writers =  "
				+ DatabaseDescriptor.getCAPIFlashCommitlogNumberOfAsyncWrite() + "Chunks:"
				+ DatabaseDescriptor.getCAPIFlashCommitLogNumberOfChunks());
		openChunks(num_async);
	}

	public SyncChunkManager() {
		this(DatabaseDescriptor.getCAPIFlashCommitLogNumberOfAsyncCallsPerChunk());
	}

	public void write(long startOffset, int num_blocks, CheckSummedBuffer buf) {
		Chunk cur = getNextChunk();
		try {
			semaphore.acquireUninterruptibly();
			cur.writeBlock(startOffset, num_blocks, buf.getBuffer());
		} catch (IOException e) {
			e.printStackTrace();
		} 
		finally{
			semaphore.release();
		}
	}
}
