package ed.inf.grape.graph;

import java.io.Serializable;

import ed.inf.grape.util.Compute;

public class Pattern extends Graph implements Serializable {

	private static final long serialVersionUID = -95796656026667561L;

	private final int x;
	private final int y;
	private final int xyEdgeType;

	private int nodeID = 0;

	public Pattern(int x, int y) {
		// TODO: modify this for different data sets.

		super();

		this.x = x;
		this.y = y;
		this.xyEdgeType = Compute.getEdgeType(y);

		Node nx = new Node(nodeID++);
		Node ny = new Node(nodeID++);

		nx.SetAttribute(x);
		ny.SetAttribute(y);

		this.InsNode(nx);
		this.InsNode(ny);
		this.InsEdge(nx, ny);
	}

	public void growsOneEdge(int fromNodeID, int targetNodeAttribute) {

		Node fromNode = this.FindNode(fromNodeID);

		Node targetNode = new Node(nodeID++);
		targetNode.SetAttribute(targetNodeAttribute);

		this.InsNode(targetNode);
		this.InsEdge(fromNode, targetNode);
	}

	public int getX() {
		return this.x;
	}

	public int getY() {
		return this.y;
	}

	public int getXYEdgeType() {
		return this.xyEdgeType;
	}

	@Override
	public String toString() {
		this.Display();
		return "Query [x=" + x + ", y=" + y + ", xyEdgeType=" + xyEdgeType
				+ "]";
	}
}
