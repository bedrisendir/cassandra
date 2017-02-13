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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.capi.util.CheckSummedBuffer;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

public abstract class ChunkManager {
	static String[] DEVICES = DatabaseDescriptor.getCAPIFlashCommitLogDevices();
	final CapiBlockDevice dev = CapiBlockDevice.getInstance();
	public static  Chunk chunks[] = new Chunk[DatabaseDescriptor.getCAPIFlashCommitLogNumberOfChunks()];
	final AtomicInteger nextChunk = new AtomicInteger(0);

	public abstract void write(long l, int m, CheckSummedBuffer buf);

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

	public void closeChunks() {
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
