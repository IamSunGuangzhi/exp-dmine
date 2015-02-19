package ed.inf.discovery;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.experimental.equivalence.EquivalenceComparator;
import org.jgrapht.experimental.isomorphism.AdaptiveIsomorphismInspectorFactory;
import org.jgrapht.experimental.isomorphism.GraphIsomorphismInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.grape.graph.Graph;
import ed.inf.grape.graph.Node;
import ed.inf.grape.util.KV;

public class Pattern implements Serializable {

	private static final long serialVersionUID = 8335552619728619513L;
	@SuppressWarnings("rawtypes")
	private static EquivalenceComparator _vComparator = new VertexComparator();
	@SuppressWarnings("rawtypes")
	private static EquivalenceComparator _eComparator = new EdgeComparator();

	/** pattern ID */
	private int patternID;

	/** origin, to get Xs */
	private int originID;

	/** origin partition ID */
	private int partitionID = 0;

	private DefaultDirectedGraph<SimpleNode, DefaultEdge> Q;
	private SimpleNode x;
	private SimpleNode y;

	private RoaringBitmap XCandidates;
	private RoaringBitmap XNotYCandidates;
	private double confidence;
//	private RoaringBitmap discoveredPartitions;

	/** for pattern growing, assign this to new node */
	private int currentNodeID = 0;

	/** for pattern growing, assign this to new pattern */
	private static int currentGloblePatternID = 0;

	static Logger log = LogManager.getLogger(Pattern.class);

	static {
		_vComparator = new VertexComparator();
		_eComparator = new EdgeComparator();
	}

	public Pattern(int partitionID) {
		this.partitionID = partitionID;
		this.patternID = Pattern.currentGloblePatternID++;
		this.originID = this.patternID;
		this.Q = new DefaultDirectedGraph<SimpleNode, DefaultEdge>(
				DefaultEdge.class);
		this.currentNodeID = 0;

		this.XCandidates = new RoaringBitmap();
		this.XNotYCandidates = new RoaringBitmap();
		confidence = 0.0;
//		this.discoveredPartitions = new RoaringBitmap();
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

		this.confidence = o.confidence;
		this.XCandidates = SerializationUtils.clone(o.XCandidates);
		this.XNotYCandidates = SerializationUtils.clone(o.XNotYCandidates);
//		this.discoveredPartitions = SerializationUtils
//				.clone(o.discoveredPartitions);
	}

	public RoaringBitmap getXCandidates() {
		return XCandidates;
	}

	public void setXCandidates(RoaringBitmap xCandidates) {
		this.XCandidates = xCandidates;
	}

	public RoaringBitmap getXNotYCandidates() {
		return XNotYCandidates;
	}

	public void setXnotYCandidates(RoaringBitmap xCandidates) {
		this.XNotYCandidates = xCandidates;
	}

	public double getConfidence() {
		return confidence;
	}

	public void setConfidence(double confidence) {
		this.confidence = confidence;
	}

//	public RoaringBitmap getDiscoveredPartitions() {
//		return discoveredPartitions;
//	}
//
//	public void setDiscoveredPartitions(RoaringBitmap discoveredPartitions) {
//		this.discoveredPartitions = discoveredPartitions;
//	}

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

	public void setCoordinatorPatternID(int id) {
		this.patternID = id;
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
		for (SimpleNode n : this.getQ().vertexSet()) {
			if (n.hop > max) {
				max = n.hop;
			}
		}
		return max;
	}

	private boolean isExtendibleInRound(int r) {
		// TODO: check is extendible in round r
		return true;
	}

	public static boolean testSamePattern(Pattern p1, Pattern p2) {

		if (p1.getQ().edgeSet().size() != p2.getQ().edgeSet().size()
				|| p1.getQ().vertexSet().size() != p2.getQ().vertexSet().size()) {
			return false;
		}

		// if (Simulation.compute_match(p.getQ(), ep.getQ())) {

		@SuppressWarnings("unchecked")
		GraphIsomorphismInspector<DefaultEdge> gii = AdaptiveIsomorphismInspectorFactory
				.createIsomorphismInspector(p1.getQ(), p2.getQ(), _vComparator,
						_eComparator);
		return gii.isIsomorphic();

	}

	public static void add(Pattern destination, Pattern addToDest) {

		// FIXME: change add method
		destination.confidence += addToDest.confidence;
		destination.XCandidates.or(addToDest.XCandidates);
		destination.XNotYCandidates.or(addToDest.XNotYCandidates);
	}

	@Override
	public String toString() {
		return "Pattern [patternID=" + patternID + ", originID=" + originID
				+ ", partitionID=" + partitionID + ", Q=" + Q + ", x=" + x
				+ ", y=" + y + ", diameter=" + getDiameter() + "]";
	}

	public Graph toGraph() {

		Graph g = new Graph();
		for (SimpleNode v : this.Q.vertexSet()) {
			Node node = new Node(v.nodeID, v.attribute);
			g.InsNode(node);
		}

		for (DefaultEdge e : this.Q.edgeSet()) {
			Node sourceNode = g.FindNode(this.Q.getEdgeSource(e).nodeID);
			Node targetNode = g.FindNode(this.Q.getEdgeTarget(e).nodeID);
			g.InsEdge(sourceNode, targetNode);
		}
		return g;
	}

	// private int getDiameter() {
	// int max = 0;
	// HashMap<SimpleNode, Integer> visited = new HashMap<SimpleNode,
	// Integer>();
	// Queue<SimpleNode> q = new LinkedList<SimpleNode>();
	//
	// for (SimpleNode vf : this.Q.vertexSet()) {
	// visited.clear();
	// q.clear();
	// q.add(vf);
	// visited.put(vf, 0);
	// while (!q.isEmpty()) {
	// SimpleNode v = q.poll();
	// int dist = visited.get(v);
	// for (DefaultEdge e : this.Q.outgoingEdgesOf(v)) {
	// SimpleNode tv = this.Q.getEdgeTarget(e);
	// if (!visited.keySet().contains(tv)) {
	// q.add(tv);
	// visited.put(tv, dist + 1);
	// }
	// }
	// for (DefaultEdge e : this.Q.incomingEdgesOf(v)) {
	// SimpleNode fv = this.Q.getEdgeSource(e);
	// if (!visited.keySet().contains(fv)) {
	// q.add(fv);
	// visited.put(fv, dist + 1);
	// }
	// }
	// }
	//
	// for (SimpleNode v : visited.keySet()) {
	// int dist = visited.get(v);
	// if (dist > max) {
	// max = dist;
	// }
	// }
	// }
	// return max;
	// }
	static class VertexComparator
			implements
			EquivalenceComparator<SimpleNode, org.jgrapht.Graph<SimpleNode, DefaultEdge>> {

		@Override
		public boolean equivalenceCompare(SimpleNode arg1, SimpleNode arg2,
				org.jgrapht.Graph<SimpleNode, DefaultEdge> context1,
				org.jgrapht.Graph<SimpleNode, DefaultEdge> context2) {
			// TODO Auto-generated method stub

			if (arg1.attribute == arg2.attribute) {
				return true;
			}
			return false;
		}

		@Override
		public int equivalenceHashcode(SimpleNode arg1,
				org.jgrapht.Graph<SimpleNode, DefaultEdge> context) {
			// TODO Auto-generated method stub
			return 0;
		}

	}

	static class EdgeComparator
			implements
			EquivalenceComparator<DefaultEdge, org.jgrapht.Graph<SimpleNode, DefaultEdge>> {

		@Override
		public boolean equivalenceCompare(DefaultEdge arg1, DefaultEdge arg2,
				org.jgrapht.Graph<SimpleNode, DefaultEdge> context1,
				org.jgrapht.Graph<SimpleNode, DefaultEdge> context2) {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public int equivalenceHashcode(DefaultEdge arg1,
				org.jgrapht.Graph<SimpleNode, DefaultEdge> context) {
			// TODO Auto-generated method stub
			return 0;
		}

	}

	public static void main(String[] args) {

		// automorphism test

		Pattern p1 = new Pattern(0);
		p1.initialXYEdge(1, 41);
		p1.expend1Node1EdgeAsChildFromFixedNode(0, 10);

		Pattern p2 = new Pattern(0);
		p2.initialXYEdge(1, 10);
		p2.expend1Node1EdgeAsChildFromFixedNode(0, 41);

		System.out.println(p1.toString());
		System.out.println(p2.toString());

		System.out.println(Pattern.testSamePattern(p1, p2));
	}
}
