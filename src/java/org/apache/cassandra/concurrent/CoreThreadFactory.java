package org.apache.cassandra.concurrent;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.cassandra.config.Config;

import io.netty.util.concurrent.FastThreadLocalThread;

public class CoreThreadFactory implements ThreadFactory {
	public CoreThreadFactory(int executorid) {
		this.coreID = executorid;
	}

	@Override
	public synchronized Thread newThread(Runnable r) {
		Thread thread = new FastThreadLocalThread(new Runnable() {
			@Override
			public void run() {	
			    Scheduler.instance.pinMe("others");
				r.run();
			}
		}, "Eventloop-" + coreID + "_" + id);
		id++;
		thread.setDaemon(true);
		return thread;
	}
}
