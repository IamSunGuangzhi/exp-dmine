package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashSet;

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

	public Partition(int partitionID) {
		super();
		this.partitionID = partitionID;
	}

	public int getPartitionID() {
		return partitionID;
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = "
				+ this.GetNodeSize() + " | edges = "
				+ this.GetEdgeSize();
	}
}
