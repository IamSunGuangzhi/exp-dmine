package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashSet;

import ed.inf.grape.util.Compute;
import ed.inf.grape.util.KV;

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

	private HashSet<Integer> X;

	private HashSet<Integer> XY;
	private HashSet<Integer> XNotY;

	private int YCount = 0;
	private int notYCount = 0;

	public Partition(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.X = new HashSet<Integer>();
		this.XY = new HashSet<Integer>();
		this.XNotY = new HashSet<Integer>();
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
}
