/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.commitlog.capi;
//package org.apache.cassandra.db.commitlog;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

public class CAPIFlashCommitLogTest {
	private static final String KEYSPACE1 = "CAPIFlashCommitLogTest";
	private static final String STANDARD1 = "Standard1";

	public CAPIFlashCommitLogTest() {
		/*
		 * if (System.getProperty("cassandra.storagedir") == null)
		 * System.setProperty("cassandra.storagedir", "cassandra/data"); if
		 * (System.getProperty("java.library.path") == null)
		 * System.setProperty("java.library.path", "cassandra/lib/sigar-bin/");
		 * if (System.getProperty("capi.devices") == null)
		 * System.setProperty("capi.devices", "/dev/sg7:0:1"); // Enabled this
		 * while using real capi flash //System.setProperty("capi.devices",
		 * "/dev/shm/capi.log:0:2"); // Enable this while using capi-sim if
		 * (System.getProperty("capi.capacity.blocks") == null)
		 * System.setProperty("capi.capacity.blocks", "524288");
		 */
	}

	@BeforeClass
	public static void beforeClass() {
		// Disable durable writes for system keyspaces to prevent system
		// mutations, e.g. sstable_activity,
		// to end up in CL segments and cause unexpected results in this test
		// wrt counting CL segments,
		// see CASSANDRA-12854
		KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;
	}

	@AfterClass
	public static void afterClass() {
	
	}

	@Before
	public void beforeTest() throws IOException {
		ByteBuffer buf = ByteBuffer.allocateDirect(4096);
		buf.putLong(0);
		//long startAddr = DatabaseDescriptor.getCAPIFlashCommitLogStartOffset();
		//int numSegments = DatabaseDescriptor.getCAPIFlashCommitLogNumberOfSegments();
		//Chunk ch = CapiBlockDevice.getInstance().openChunk(DatabaseDescriptor.getCAPIFlashCommitLogDevices()[0]);
		long startAddr = 0;
		int numSegments = 256;
		Chunk ch = CapiBlockDevice.getInstance().openChunk("/dev/sg0");
		System.err.println("start=" + startAddr + " blocks=" + numSegments);
		int ctr = 0;
		for (int x = 0; x < numSegments; x++) {
			ch.writeBlock(startAddr + x, 1, buf);
			ctr++;
		}
		System.err.println("Cleared " + ctr + " blocks!");	
		SchemaLoader.prepareServer();
		SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1),
				SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));
	}

	@After
	public void afterTest() {
	}

	@Test
	public void replaySimple() throws IOException {
		int cellCount = 0;
		ColumnFamilyStore cfs = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
		final Mutation rm1 = new RowUpdateBuilder(cfs.metadata(), 0, "k1").clustering("bytes")
				.add("val", bytes("this is a string")).build();
		cellCount += 1;
		CAPIFlashCommitLog.instance.add(rm1);
		final Mutation rm2 = new RowUpdateBuilder(cfs.metadata(), 0, "k2").clustering("bytes")
				.add("val", bytes("this is a string")).build();
		cellCount += 1;
		CAPIFlashCommitLog.instance.add(rm2);
		System.err.println(CAPIFlashCommitLog.instance.getCurrentPosition());
		
		CAPIFlashCommitLog.instance.reset();
		
		int recoverCount = CAPIFlashCommitLog.instance.recover();
		assertEquals(cellCount, recoverCount);
	}

}