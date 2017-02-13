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
package org.apache.cassandra.db.commitlog.capi.bufferallocator;

import org.apache.cassandra.db.commitlog.capi.util.CheckSummedBuffer;
import org.apache.cassandra.db.commitlog.capi.util.SimpleCachedCheckSummedBufferPool;
import org.apache.cassandra.io.compress.BufferType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PooledAllocationStrategy implements BufferAllocationStrategy {
	static final Logger logger = LoggerFactory.getLogger(PooledAllocationStrategy.class);

	final SimpleCachedCheckSummedBufferPool[] cachedPools = new SimpleCachedCheckSummedBufferPool[1024];
	static int maxBufferperPool = 256;
	int largeBufferLimit = 32;

	public PooledAllocationStrategy(int bufferCount) {
		maxBufferperPool = bufferCount;
	}

	@Override
	public CheckSummedBuffer poll(long requiredBlocks) {
		int value = (int) (requiredBlocks);
		if (cachedPools[value] == null) {
			if (value < largeBufferLimit) {
				cachedPools[value] = new SimpleCachedCheckSummedBufferPool(maxBufferperPool, value * 4096);
			} else {
				cachedPools[value] = new SimpleCachedCheckSummedBufferPool(0, value * 4096);
			}
		}

		return cachedPools[value].createBuffer(BufferType.OFF_HEAP);
	}

	@Override
	public void free(CheckSummedBuffer buf) {
		cachedPools[buf.blocks].releaseBuffer(buf);
	}
}
