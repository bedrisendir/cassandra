package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.stream.IntStream;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.PointerType;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.PointerByReference;

public class ThreadPinner {
	private static final Object[] NO_ARGS = {};
	CLibrary INSTANCE = null;
	static int MAX_CPUSET_SIZE = 128 * 8;
	HashMap<Integer, ArrayList<Integer>> cpuList = new HashMap<Integer, ArrayList<Integer>>();
	Integer num_nodes = -1;
	Integer smt = -1;

	interface CLibrary extends Library {
		int sched_setaffinity(final int pid, final int cpusetsize, final PointerType cpuset) throws LastErrorException;

		int sched_getaffinity(final int pid, final int cpusetsize, final PointerType cpuset) throws LastErrorException;

		int sched_getcpu() throws LastErrorException;

		int getcpu(final IntByReference cpu, final IntByReference node, final PointerType tcache)
				throws LastErrorException;

		int syscall(int number, Object... args) throws LastErrorException;
	}

	public ThreadPinner() {
		INSTANCE = (CLibrary) Native.loadLibrary("c", CLibrary.class);
		// getNuma();
	}

	public int getCPU() {
		return INSTANCE.sched_getcpu();
	}

	public int getThreadID() {
		return INSTANCE.syscall(207, NO_ARGS);
	}

	/**
	 * @param values
	 *            list of cpu's to bind
	 * @return return code
	 */
	public int bindToCpus(int ThreadID, int[] values) {
		BitSet mask = new BitSet(MAX_CPUSET_SIZE);
		for (int i : values) {
			mask.set(i);
		}
		return bind(ThreadID, mask);
	}

	/**
	 * @param socket
	 *            numa node to bind cores from
	 * @param values
	 *            list of cores to bind in defined socket
	 * @return
	 */
	public int bindToCores(int ThreadID, int socket, int[] values) {
		BitSet mask = new BitSet(MAX_CPUSET_SIZE);
		for (int z = 0; z < cpuList.get(socket).size(); z += smt) {
			for (int i = z; z < (smt * z); i++) {
				mask.set(cpuList.get(socket).get(i));
			}
		}
		return bind(ThreadID, mask);
	}

	/**
	 * Method to bind thread to all cores in given sockets
	 * 
	 * @param values
	 * @return
	 */
	public int bindToNodes(int ThreadID, int[] values) {
		BitSet mask = new BitSet(MAX_CPUSET_SIZE);
		for (int i : values) {
			for (int cpu : cpuList.get(i)) {
				mask.set(cpu);
			}
		}
		return bind(ThreadID, mask);
	}

	/**
	 * @param socket
	 *            numa node to bind cores from
	 * @param values
	 *            list of cores to bind in defined socket
	 * @return
	 */
	public int bindToCores(int socket, int[] values) {
		return bindToCores(0, socket, values);
	}

	/**
	 * Method to bind thread to all cores in given sockets
	 * 
	 * @param values
	 * @return
	 */
	public int bindToNodes(int[] values) {
		return bindToNodes(0, values);
	}

	/**
	 * @param values
	 *            list of cpu's to bind
	 * @return return code
	 */
	public int bindToCpus(int[] values) {
		System.err.println(Thread.currentThread().getName() + " " + java.util.Arrays.toString(values));
		return bindToCpus(0, values);
	}

	private int bind(int threadID, BitSet mask) {
		byte[] arr = mask.toByteArray();
		Pointer ptr = new Memory(arr.length);
		ptr.write(0, arr, 0, arr.length);
		PointerByReference pt = new PointerByReference(ptr);
		pt.setPointer(ptr);
		return INSTANCE.sched_setaffinity(threadID, arr.length, pt);
	}

	/**
	 * @return
	 */
	public String getInfo() {
		StringBuilder build = new StringBuilder();

		return build.toString();
	}

	public int bindToCore(int core) {
		return bindToCpus(IntStream.iterate(core * 8, i -> i + 1).limit(8).toArray());
	}

}