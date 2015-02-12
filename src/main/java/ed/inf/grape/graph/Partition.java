package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.omg.CORBA.FREE_MEM;

import ed.inf.grape.util.Compute;
import ed.inf.grape.util.IO;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Partition extends Graph implements Serializable {

	private static final long serialVersionUID = -4757004627010733180L;

	private int partitionID;

	/** Node set in BF functions */

	private HashSet<Integer> X;

	private HashSet<Integer> XY;
	private HashSet<Integer> XNotY;

	private int YCount = 0;
	private int notYCount = 0;

	/** Statistics of current partition */

	private Map<IndexEdge, Integer> freqEdge;

	static Logger log = LogManager.getLogger(Partition.class);

	public Partition(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.X = new HashSet<Integer>();
		this.XY = new HashSet<Integer>();
		this.XNotY = new HashSet<Integer>();

		this.freqEdge = new HashMap<IndexEdge, Integer>();
	}

	public int getPartitionID() {
		return partitionID;
	}

	/**
	 * First compute step, initial select and count.
	 * 
	 * @param xLabel
	 * @param yLabel
	 * @param xyEdgeType
	 */
	public void initCount(int xLabel, int yLabel, int xyEdgeType) {

		/** count X, Y, XY, notY */

		for (Node node : this.GetNodeSet()) {
			if (node.GetAttribute() == xLabel) {
				X.add(node.GetID());
			}
		}

		for (int nodeID : this.X) {

			/** count x with edge xy and xnotybutother */
			/** a x with edge xy count in xy but not xnoty */

			boolean hasXYEdgeType = false;
			boolean hasXY = false;

			for (Node childNode : this.GetChildren(this.FindNode(nodeID))) {
				if (Compute.getEdgeType(childNode.GetAttribute()) == xyEdgeType) {
					hasXYEdgeType = true;
					if (childNode.GetAttribute() == yLabel) {
						XY.add(nodeID);
						hasXY = true;
						YCount++;
					} else {
						notYCount++;
					}
				}
			}

			if (hasXYEdgeType == true && hasXY == false) {
				XNotY.add(nodeID);
			}
		}

		/** build frequent-edge Index */

		log.info("frequent edge size = " + this.freqEdge.size());
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = "
				+ this.GetNodeSize() + " | edges = " + this.GetEdgeSize();
	}

	public String getCountInfo() {
		return "X.size = " + this.X.size() + " | XY.size = " + this.XY.size()
				+ " | XNotY.size = " + this.XNotY.size() + " | YCount = "
				+ this.YCount + " | notYCount = " + this.notYCount;
	}

	public void setFreqEdge(Map<IndexEdge, Integer> map) {
		this.freqEdge = map;
	}

	public Map<IndexEdge, Integer> getFreqEdge() {
		return this.freqEdge;
	}

	private void makeFreqEdgeIndex() {

		System.out.println("begin build.");
		Queue<Integer> toVisit = new LinkedList<Integer>();
		HashSet<Integer> visited = new HashSet<Integer>();

		int edgecount = 0;

		for (int nodeID : this.X) {
			toVisit.add(nodeID);
		}

		while (toVisit.size() > 0) {

			int nodeID = toVisit.poll();
			Node node = this.FindNode(nodeID);
			for (Node childNode : this.GetChildren(node)) {

				IndexEdge e = new IndexEdge(node.GetAttribute(),
						childNode.GetAttribute());
				if (!freqEdge.containsKey(e)) {
					freqEdge.put(e, 0);
				}
				freqEdge.put(e, freqEdge.get(e) + 1);
				edgecount++;
				if (edgecount % 100000 == 0) {
					log.debug("iterated " + edgecount + " edges.");
				}

				if (toVisit.contains(childNode.GetID())
						|| visited.contains(childNode.GetID())) {
					continue;
				} else {
					toVisit.add(childNode.GetID());
				}
			}
			visited.add(nodeID);
		}

		int t = 0;
		for (IndexEdge edge : freqEdge.keySet()) {
			System.out.println(edge);
			t++;
			if (t > 300) {
				break;
			}
		}

		log.debug("total edge count = " + edgecount);
	}
}
