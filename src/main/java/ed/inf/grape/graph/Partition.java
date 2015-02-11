package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashSet;

import org.jgrapht.graph.DefaultDirectedWeightedGraph;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Partition extends DefaultDirectedWeightedGraph<Integer, Edge>
		implements Serializable {

	private static final long serialVersionUID = -4757004627010733180L;

	private int partitionID;
	private HashSet<Integer> incomingVertices;
	private HashSet<Integer> outgoingVertices;

	public Partition(int partitionID) {
		super(Edge.class);
		this.partitionID = partitionID;
		this.incomingVertices = new HashSet<Integer>();
		this.outgoingVertices = new HashSet<Integer>();
	}

	public int getPartitionID() {
		return partitionID;
	}

	public Edge addEdgeWith2Endpoints(Integer sourceVertex, Integer targetVertex) {

		if (!this.containsVertex(sourceVertex)) {
			this.addVertex(sourceVertex);
		}

		if (!this.containsVertex(targetVertex)) {
			this.addVertex(targetVertex);
		}

		return this.addEdge(sourceVertex, targetVertex);
	}

	public HashSet<Integer> getIncomingVertices() {
		return incomingVertices;
	}

	public HashSet<Integer> getOutgoingVertices() {
		return outgoingVertices;
	}

	public boolean isInnerVertex(int vertex) {
		return (!outgoingVertices.contains(vertex))
				&& (!incomingVertices.contains(vertex));
	}

	public void addOutgoingVertex(Integer vertex) {
		this.outgoingVertices.add(vertex);
	}

	public void addIncomingVertex(Integer vertex) {
		this.incomingVertices.add(vertex);
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = "
				+ this.vertexSet().size() + " | edges = "
				+ this.edgeSet().size() + " | iv = " + incomingVertices.size()
				+ " | ov = " + outgoingVertices.size();
	}
}
