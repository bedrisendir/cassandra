package org.apache.cassandra.concurrent;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.primitives.Ints;

public class Scheduler {
	final static Scheduler instance = new Scheduler();
	final AtomicInteger lastSharedOff = new AtomicInteger(0);
	final AtomicInteger lastOthersOff = new AtomicInteger(0);
	final ThreadPinner pinner = new ThreadPinner();

	volatile boolean optimized = false;

	/* Cpu list */
	public final List<Integer> sharedL;
	public final List<Integer> othersL;

	/* Core list */
	final String[] others;
	final String[] shared;

	final String sharedPolicy = System.getProperty("sharedPolicy");
	final String othersPolicy = System.getProperty("othersPolicy");

	Scheduler() {
		synchronized (this) {
			sharedL = new LinkedList<Integer>();
			othersL = new LinkedList<Integer>();

			String opt = System.getProperty("optimized");
			if ((opt != null) && (opt.compareTo("true") == 0)) {
				System.err.println("!!!!!!!!optimized set to true");
				optimized = true;
			}
			if (optimized) {
				shared = System.getProperty("shared").split(":");
				System.err.println(shared);
				for (String str : shared) {
					System.out.println("-->" + str);
					int startcpu = Integer.valueOf(str) * 8;
					for (int i = startcpu; i < (startcpu + 8); i++) {
						sharedL.add(i);
					}
				}
				others = System.getProperty("others").split(":");
				System.err.println(others);
				for (String str : others) {
					System.out.println("-->" + str);
					int startcpu = Integer.valueOf(str) * 8;
					for (int i = startcpu; i < (startcpu + 8); i++) {
						othersL.add(i);
					}
				}
			} else {
				others = null;
				shared = null;
			}
		}
	}

	public void pinMe(String tag) {
		System.err.println(Thread.currentThread().getName() + " " + optimized);
		if (optimized) {
			if (tag.compareTo("shared") == 0) {
				System.err.println(Thread.currentThread().getName() + "shared pin");
				pinShared();
			} else if (tag.compareTo("others") == 0) {
				System.err.println(Thread.currentThread().getName() + "others pin");
				pinOthers();
			}
		}
	}

	private void pinOthers() {
		if (othersPolicy.compareTo("seq") == 0) {
			System.err.println(Thread.currentThread().getName() + "others pin sq");
			pinner.bindToCore(Integer.valueOf(others[Math.abs(lastOthersOff.getAndIncrement() % others.length)]));
		} else if (othersPolicy.compareTo("range") == 0) {
			System.err.println(Thread.currentThread().getName() + "others pin range");
			pinner.bindToCpus(Ints.toArray(othersL));
		}
	}

	private void pinShared() {
		if (sharedPolicy.compareTo("seq") == 0) {
			System.err.println(Thread.currentThread().getName() + "shared pin sq");
			pinner.bindToCore(Integer.valueOf(shared[Math.abs(lastSharedOff.getAndIncrement() % shared.length)]));
		} else if (sharedPolicy.compareTo("range") == 0) {
			System.err.println(Thread.currentThread().getName() + "shread pin range");
			pinner.bindToCpus(Ints.toArray(sharedL));
		}
	}
}
