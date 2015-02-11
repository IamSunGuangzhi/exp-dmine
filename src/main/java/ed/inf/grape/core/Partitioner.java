package ed.inf.grape.core;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.IO;
import ed.inf.grape.util.KV;

/**
 * Partitioner, divide a whole graph into several partitions with predefined
 * strategy.
 * 
 * TODO: make it interface, implements by different strategy, invoke by
 * reflection.
 * 
 * @author yecol
 *
 */
public class Partitioner {

	/** Partition graph with a simple strategy, greedy scan and divide. */
	public static int STRATEGY_SIMPLE = 0;

	/** Partition graph with LibMetis. */
	public static int STRATEGY_METIS = 1;

	/** Partition graph with hash vertex. */
	public static int STRATEGY_HASH = 2;

	/** Partition strategy */
	private int strategy;

	/** Partition id */
	private static int currentPartitionId;

	static Logger log = LogManager.getLogger(Partitioner.class);

	public Partitioner(int strategy) {
		this.strategy = strategy;
	}

	public int getNumOfPartitions() {
		return KV.PARTITION_COUNT;
	}

	public boolean hasNextPartitionID() {

		return currentPartitionId < KV.PARTITION_COUNT;
	}

	public int getNextPartitionID() {

		/** Assume have run program target/gpartition */

		assert this.strategy == STRATEGY_METIS;

		int ret = -1;

		if (currentPartitionId < KV.PARTITION_COUNT) {
			ret = currentPartitionId++;
		}

		return ret;
	}

	public Map<Integer, Integer> getVirtualVertex2PartitionMap() {

		assert this.strategy == STRATEGY_METIS;

		try {
			return IO.loadInt2IntMapFromFile(KV.GRAPH_FILE_PATH + ".vvp");
		} catch (IOException e) {
			log.error("load virtual vertex 2 partition map failed.");
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {

		Partitioner partitioner = new Partitioner(STRATEGY_METIS);

		int p = partitioner.getNextPartitionID();

		while (p != -1) {

			log.info("partitionID=" + p);
			p = partitioner.getNextPartitionID();
			// p = partitioner.getNextPartition();
		}
	}

	public static int hashVertexToPartition(int vertexID) {

		assert KV.PARTITION_STRATEGY == STRATEGY_HASH;
		// TODO:hash and map vertex to partition;
		return -1;
	}
}
