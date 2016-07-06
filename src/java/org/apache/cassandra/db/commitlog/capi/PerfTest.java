package org.apache.cassandra.db.commitlog.capi;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.MigrationManager;

public class PerfTest {
	static String keyspace = "stress";
	static String column_family = "columnfamily";
	static Integer key_length = 16;
	static Integer field_count = 10;
	static Integer field_length = 100;
	static Integer total_records = 10000000;
	static boolean durable_writes = false;
	static Integer insertThreads = 32;
	Thread threads[] = new Thread[insertThreads];

	public void initKeyspace() {
		// Keyspace params
		HashMap<String, String> rep = new HashMap<String, String>();
		rep.put("class", "SimpleStrategy");
		rep.put("replication_factor", "1");
		KeyspaceParams ksp = KeyspaceParams.create(durable_writes, rep); // durable writes
		// Table params
		CFMetaData.Builder builder = CFMetaData.Builder.create(keyspace, column_family).addPartitionKey("y_id",
				BytesType.instance);
		for (int i = 0; i < field_count; i++) {
			builder.addRegularColumn("col" + i, BytesType.instance);
		}
		CFMetaData cfp = builder.build().compression(CompressionParams.noCompression());
		MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(keyspace, ksp, Tables.of(cfp)), true);
	}

	public void initThreads() {

		for (int i = 0; i < threads.length; i++) {
			threads[i] = new Thread(new WorkloadTask(total_records / insertThreads));
			threads[i].setName("insert-" + i);
		}
	}

	public void insert() {
		initKeyspace();
		initThreads();
		long start = System.currentTimeMillis();
		for (Thread t : threads) {
			t.start();
		}
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("insert finished: " + ((float)(end-start))/1000);
		
	}

	class WorkloadTask implements Runnable {
		final byte[] key = new byte[key_length];
		final byte[] bytes = new byte[field_length];
		int records = 0;

		WorkloadTask(int n_records) {
			records = n_records;
		}

		@Override
		public void run() {
			for (int j = 0; j < records; j++) {
				ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(column_family);
				final byte[] key = new byte[key_length];
				ThreadLocalRandom.current().nextBytes(key);
				RowUpdateBuilder update = new RowUpdateBuilder(cfs.metadata, System.currentTimeMillis(),
						ByteBuffer.wrap(key));
				// .clustering("bytes");
				for (int i = 0; i < field_count; i++) {
					final byte[] bytes = new byte[field_length];
					ThreadLocalRandom.current().nextBytes(bytes);
					update.add("col" + i, ByteBuffer.wrap(bytes));
				}
				Keyspace.open(keyspace).apply(new Mutation(update.buildUpdate()), durable_writes);
			}
		}
	}
}
