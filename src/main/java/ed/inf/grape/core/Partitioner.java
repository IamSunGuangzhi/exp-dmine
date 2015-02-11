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

	// @SuppressWarnings("unused")
	// private List<Partition> simplePartition() throws IOException {
	//
	// /** approximately compute the size of each partition */
	// int sizeOfPartition = GRAPH_VERTEX_COUNT / PARTITION_COUNT + 1;
	//
	// log.debug("graph_vertex count = " + GRAPH_VERTEX_COUNT);
	// log.debug("size of partition = " + sizeOfPartition);
	//
	// /** the whole graph */
	// cg_graph g = IO.loadGraphWithStreamScanner(GRAPH_FILE_PATH);
	//
	// /** vertices 2 partitionId */
	// Map<String, Integer> verticesInPartition = new HashMap<String,
	// Integer>();
	//
	// List<Partition> partitions = new ArrayList<Partition>();
	//
	// /** init partitions */
	// for (int i = 0; i < PARTITION_COUNT; i++) {
	// Partition partition = new Partition(currentPartitionId++);
	// partitions.add(partition);
	// }
	//
	// /** partition vertices by greedy strategy */
	// Set<String> unPartitionedVertices = new HashSet<String>();
	// unPartitionedVertices.addAll(g.vertexSet());
	// Iterator<String> iter = null;
	//
	// Queue<String> toPartitionedVertices = new LinkedList<String>();
	//
	// int i = 0;
	//
	// while (!unPartitionedVertices.isEmpty()) {
	//
	// iter = unPartitionedVertices.iterator();
	//
	// String seedv = iter.next();
	// toPartitionedVertices.add(seedv);
	//
	// while (!toPartitionedVertices.isEmpty()) {
	// String v = toPartitionedVertices.poll();
	// try {
	// if (unPartitionedVertices.contains(v)) {
	// unPartitionedVertices.remove(v);
	// int pId = i++ / sizeOfPartition;
	// partitions.get(pId).addVertex(v);
	// verticesInPartition.put(v, pId);
	// toPartitionedVertices.addAll(Graphs
	// .neighborListOf(g, v));
	// }
	// } catch (Exception e) {
	// log.error(e.getStackTrace());
	// log.error(i + "-" + sizeOfPartition);
	// }
	// }
	// }
	//
	// log.debug("vertices partition finished.");
	//
	// int innerEdge = 0;
	//
	// /** add edges to each partition */
	// for (edge e : g.edgeSet()) {
	// String vsource = g.getEdgeSource(e);
	// String vtarget = g.getEdgeTarget(e);
	// int pIDOfSource = verticesInPartition.get(vsource);
	// int pIDOfTarget = verticesInPartition.get(vtarget);
	// if (pIDOfSource == pIDOfTarget) {
	// innerEdge++;
	// partitions.get(pIDOfSource).addEdge(vsource, vtarget);
	// } else {
	// partitions.get(pIDOfSource).addOutgoingVertex(vtarget);
	// partitions.get(pIDOfTarget).addIncomingVertex(vsource);
	// }
	// }
	//
	// /*
	// * FIXME:
	// *
	// * 1.only 37% are inner edges in amazon data set and 53% in YouTube data
	// * set. 2.incoming and outgoing map need adjust. or will lose info.
	// *
	// * edges=8112707 innerEdge=3031417 ratio=0.3736628230256559
	// */
	//
	// log.debug("edges=" + g.edgeSet().size() + " innerEdge=" + innerEdge
	// + " ratio=" + innerEdge * 1.0 / g.edgeSet().size());
	// ;
	//
	// log.debug("edges partition finished.");
	//
	// for (Partition p : partitions) {
	// log.info(p.getPartitionInfo());
	// }
	//
	// return partitions;
	// }

	public Partition getNextPartition() {

		/** Assume have run program target/gpartition */

		assert this.strategy == STRATEGY_METIS;

		Partition p = null;

		if (currentPartitionId >= KV.PARTITION_COUNT) {
			return p;
		}

		else {

			String partitionFilename = KV.GRAPH_FILE_PATH + ".p"
					+ String.valueOf(currentPartitionId);
			p = IO.loadPartitions(currentPartitionId++, partitionFilename);
		}
		log.info(p.getPartitionInfo());

		return p;
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
