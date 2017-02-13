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

import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.capi.util.CheckSummedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedAllocationStrategy implements BufferAllocationStrategy {
	static final Logger logger = LoggerFactory.getLogger(FixedAllocationStrategy.class);
	private final ConcurrentLinkedQueue<CheckSummedBuffer> buffers = new ConcurrentLinkedQueue<CheckSummedBuffer>();

	public FixedAllocationStrategy() {
		for (int i = 0; i < DatabaseDescriptor.getCAPIFlashCommitlogNumberOfBuffers(); i++) {
			buffers.add(new CheckSummedBuffer(DatabaseDescriptor.getCAPIFlashCommitLogBufferSizeInBlocks()*4096));
		}
	}

	@Override
	public CheckSummedBuffer poll(long requiredBlocks) {
		CheckSummedBuffer ret = null;
		// busy wait if resource is not available
		while ((ret = buffers.poll()) == null);
		return ret;
	}

	@Override
	public void free(CheckSummedBuffer buf) {
		buf.clear();
		buffers.add(buf);
	}
}
