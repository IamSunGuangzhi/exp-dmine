package ed.inf.discovery;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.grape.util.KV;

public class Pattern implements Serializable {

	/** pattern ID */
	private int patternID;

	/** origin, to get Xs */
	private int originID;

	/** origin partition ID */
	private int partitionID = 0;

	private DefaultDirectedGraph<SimpleNode, DefaultEdge> Q;
	private SimpleNode x;
	private SimpleNode y;

	/** for pattern growing, assign this to new node */
	private int currentNodeID = 0;

	/** for pattern growing, assign this to new pattern */
	private static int currentGloblePatternID = 0;

	static Logger log = LogManager.getLogger(Pattern.class);

	public Pattern(int partitionID) {
		this.partitionID = partitionID;
		this.patternID = Pattern.currentGloblePatternID++;
		this.originID = this.patternID;
		this.Q = new DefaultDirectedGraph<SimpleNode, DefaultEdge>(
				DefaultEdge.class);
		this.currentNodeID = 0;
	}

	public Pattern(int partitionID, Pattern o) {
		this.partitionID = partitionID;
		this.patternID = Pattern.currentGloblePatternID++;
		this.originID = o.getPatternID();
		this.Q = new DefaultDirectedGraph<SimpleNode, DefaultEdge>(
				DefaultEdge.class);
		Graphs.addGraph(this.Q, o.Q);
		this.x = o.x;
		this.y = o.y;
		this.currentNodeID = o.currentNodeID;
	}

	public void initialXYEdge(int xAttr, int yAttr) {

		SimpleNode nodex = new SimpleNode(this.nextNodeID(), xAttr, 0);
		SimpleNode nodey = new SimpleNode(this.nextNodeID(), yAttr, 1);

		this.Q.addVertex(nodex);
		this.Q.addVertex(nodey);

		this.Q.addEdge(nodex, nodey);

		this.x = nodex;
		this.y = nodey;
	}

	public void expend1Node1EdgeAsChildFromFixedNode(int fromNodeID, int toAttr) {

		for (SimpleNode fromNode : this.Q.vertexSet()) {
			if (fromNode.nodeID == fromNodeID) {
				SimpleNode toNode = new SimpleNode(this.nextNodeID(), toAttr,
						fromNode.hop + 1);
				this.Q.addVertex(toNode);
				this.Q.addEdge(fromNode, toNode);
				return;
			}
		}
	}

	public void expend1Node1EdgeAsParentFromFixedNode(int toNodeID, int toAttr) {

		for (SimpleNode toNode : this.Q.vertexSet()) {
			if (toNode.nodeID == toNodeID) {
				SimpleNode fromNode = new SimpleNode(this.nextNodeID(), toAttr,
						toNode.hop + 1);
				this.Q.addVertex(fromNode);
				this.Q.addEdge(fromNode, toNode);
				return;
			}
		}
	}

	public int getPartitionID() {
		return this.partitionID;
	}

	public int getPatternID() {
		return this.patternID;
	}

	public int getOriginID() {
		return this.originID;
	}

	public SimpleNode getX() {
		return this.x;
	}

	public SimpleNode getY() {
		return this.y;
	}

	public boolean isValid() {

		log.debug("pattern" + this.patternID + "-diameter = "
				+ this.getDiameter());

		/** diameter gt bound. */
		if (this.getDiameter() > KV.PARAMETER_B) {
			return false;
		}
		// TODO: other test.

		return true;
	}

	public DefaultDirectedGraph<SimpleNode, DefaultEdge> getQ() {
		return this.Q;
	}

	private int nextNodeID() {
		return this.currentNodeID++;
	}

	private int getDiameter() {
		int max = 0;
		HashMap<SimpleNode, Integer> visited = new HashMap<SimpleNode, Integer>();
		Queue<SimpleNode> q = new LinkedList<SimpleNode>();

		for (SimpleNode vf : this.Q.vertexSet()) {
			visited.clear();
			q.clear();
			q.add(vf);
			visited.put(vf, 0);
			while (!q.isEmpty()) {
				SimpleNode v = q.poll();
				int dist = visited.get(v);
				for (DefaultEdge e : this.Q.outgoingEdgesOf(v)) {
					SimpleNode tv = this.Q.getEdgeTarget(e);
					if (!visited.keySet().contains(tv)) {
						q.add(tv);
						visited.put(tv, dist + 1);
					}
				}
				for (DefaultEdge e : this.Q.incomingEdgesOf(v)) {
					SimpleNode fv = this.Q.getEdgeSource(e);
					if (!visited.keySet().contains(fv)) {
						q.add(fv);
						visited.put(fv, dist + 1);
					}
				}
			}

			for (SimpleNode v : visited.keySet()) {
				int dist = visited.get(v);
				if (dist > max) {
					max = dist;
				}
			}
		}
		return max;
	}

	@Override
	public String toString() {
		return "Pattern [patternID=" + patternID + ", originID=" + originID
				+ ", partitionID=" + partitionID + ", Q=" + Q + ", x=" + x
				+ ", y=" + y + ", diameter=" + getDiameter() + "]";
	}

}
