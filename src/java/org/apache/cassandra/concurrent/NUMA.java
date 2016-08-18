package org.apache.cassandra.concurrent;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.NativeLibrary;
import com.sun.jna.Pointer;

public class NUMA {
	public interface NUMALibrary extends Library {
		NUMALibrary instance = (NUMALibrary) Native.loadLibrary("numa", NUMALibrary.class);
		//http://linux.die.net/man/3/numa
		/*
		 * Before any other calls in this library can be used numa_available()
		 * must be called. If it returns -1, all other functions in this library
		 * are undefined.
		 */
		int numa_available();

		/*
		 * returns the highest node number available on the current system. (See
		 * the node numbers in /sys/devices/system/node/ ). Also see
		 * numa_num_configured_nodes().
		 */
		int numa_max_node();

		/*
		 * returns the number of the highest possible node in a system. In other
		 * words, the size of a kernel type nodemask_t (in bits) minus 1. This
		 * number can be gotten by calling numa_num_possible_nodes() and
		 * subtracting 1.
		 */
		int numa_max_possible_node();

		/*
		 * returns the size of kernel's node mask (kernel type nodemask_t). In
		 * other words, large enough to represent the maximum number of nodes
		 * that the kernel can handle. This will match the kernel's MAX_NUMNODES
		 * value. This count is derived from /proc/self/status, field
		 * Mems_allowed.
		 */
		int numa_num_possible_nodes();

		/*
		 * returns the number of memory nodes in the system. This count includes
		 * any nodes that are currently disabled. This count is derived from the
		 * node numbers in /sys/devices/system/node. (Depends on the kernel
		 * being configured with /sys (CONFIG_SYSFS)).
		 */
		int numa_num_configured_nodes();

		/*
		 * returns the number of cpus in the system. This count includes any
		 * cpus that are currently disabled. This count is derived from the cpu
		 * numbers in /sys/devices/system/cpu. If the kernel is configured
		 * without /sys (CONFIG_SYSFS=n) then it falls back to using the number
		 * of online cpus.
		 */
		int numa_num_configured_cpus();
		
	}
	
	void getCPUs(){
		Pointer ptr = NativeLibrary.getInstance("numa").getGlobalVariableAddress("numa_all_cpus_ptr");
		ptr.getByteBuffer(0, 16);
	}

	void print() {
		System.out.println("Numa Available:" + NUMALibrary.instance.numa_available());
		System.out.println("Numa Max Node:" + NUMALibrary.instance.numa_max_node());
		System.out.println("Numa Max Possible Node:" + NUMALibrary.instance.numa_max_possible_node());
		System.out.println("Numa Number of Possible Nodes:" + NUMALibrary.instance.numa_num_possible_nodes());
		System.out.println("Numa Number of Configured Nodes:" + NUMALibrary.instance.numa_num_configured_nodes());
		System.out.println("Numa Number of Configured CPUs:" + NUMALibrary.instance.numa_num_configured_cpus());
		/////////////////////////////////////////
		System.out.println("Numa Available:" + NUMALibrary.instance.numa_available());
		System.out.println("Numa Available:" + NUMALibrary.instance.numa_available());

	}
}