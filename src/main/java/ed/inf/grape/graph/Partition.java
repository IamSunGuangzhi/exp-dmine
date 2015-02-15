package ed.inf.grape.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.Pattern;
import ed.inf.discovery.auxiliary.SimpleEdge;
import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.grape.util.Compute;
import ed.inf.grape.util.IO;
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

	/** Node set in BF functions */

	private RoaringBitmap X;

	private RoaringBitmap XY;
	private RoaringBitmap XNotY;

	private int YCount = 0;
	private int notYCount = 0;

	/** pattern and its valid Xs */
	private HashMap<Integer, RoaringBitmap> XYBitmapForPatterns;
	private HashMap<Integer, RoaringBitmap> XNotYBitmapForPatterns;

	/** Statistics of current partition */

	private Map<SimpleEdge, Integer> freqEdge;

	static Logger log = LogManager.getLogger(Partition.class);

	public Partition(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.X = new RoaringBitmap();
		this.XY = new RoaringBitmap();
		this.XNotY = new RoaringBitmap();

		this.freqEdge = new HashMap<SimpleEdge, Integer>();
		this.XYBitmapForPatterns = new HashMap<Integer, RoaringBitmap>();
		this.XNotYBitmapForPatterns = new HashMap<Integer, RoaringBitmap>();
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
	public void initWithPattern(Pattern pattern) {

		/** count X, Y, XY, notY */
		for (Node node : this.GetNodeSet()) {
			if (node.GetAttribute() == pattern.getX().attribute) {
				X.add(node.GetID());
			}
		}

		for (int nodeID : this.X) {

			/** count x with edge xy and xnoty but other */
			/** a x with edge xy count in xy but not xnoty */

			boolean hasXYEdgeType = false;
			boolean hasXY = false;

			for (Node childNode : this.GetChildren(this.FindNode(nodeID))) {
				if (Compute.getEdgeType(childNode.GetAttribute()) == Compute
						.getEdgeType(pattern.getY().attribute)) {
					hasXYEdgeType = true;
					if (childNode.GetAttribute() == pattern.getY().attribute) {
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

		this.XYBitmapForPatterns.put(pattern.getPatternID(), XY);
		this.XNotYBitmapForPatterns.put(pattern.getPatternID(), XNotY);

		pattern.getXCandidates().or(XY);
	}

	public int getYCount() {
		return YCount;
	}

	public int getNotYCount() {
		return notYCount;
	}

	public int matchR(Pattern pattern) {

		// System.out.println(pattern);

		/** using x->y */

		// FIXME: not sure works correct.

		long start = System.currentTimeMillis();

		if (!this.XYBitmapForPatterns.containsKey(pattern.getOriginID())) {
			log.error("XBitMapKey Error.");
			return 0;
		}

		RoaringBitmap xset = new RoaringBitmap();

		/** Map storing edges to be mapping. HopFromX -> Edges */
		HashMap<Integer, HashSet<DefaultEdge>> oMappingEdges = new HashMap<Integer, HashSet<DefaultEdge>>();

		for (DefaultEdge e : pattern.getQ().edgeSet()) {
			int hop = pattern.getQ().getEdgeTarget(e).hop;
			if (!oMappingEdges.containsKey(hop)) {
				oMappingEdges.put(hop, new HashSet<DefaultEdge>());
			}
			oMappingEdges.get(hop).add(e);
		}
		// log.debug("match-debug" + oMappingEdges);

		for (int x : XYBitmapForPatterns.get(pattern.getOriginID()).toArray()) {

			// log.debug("match-debug" + "current x= " + x);
			HashMap<Integer, HashSet<DefaultEdge>> mappingEdges = SerializationUtils
					.clone(oMappingEdges);

			boolean satisfy = true;

			/** Map storing edges to be mapping. PatternNodeID -> GraphNodeID */
			HashSet<Integer> lastMatches = new HashSet<Integer>();
			lastMatches.add(x);

			for (int i = 1; i <= KV.PARAMETER_B; i++) {

				// System.out.println("hop = " + i);
				if (!mappingEdges.containsKey(i)) {
					// System.out.println("checked all");
					break;
				}

				HashSet<Integer> currentMatches = new HashSet<Integer>();
				for (DefaultEdge e : mappingEdges.get(i)) {
					boolean edgeSatisfy = false;
					for (int lmatch : lastMatches) {

						if (this.FindNode(lmatch).GetAttribute() == pattern
								.getQ().getEdgeSource(e).attribute) {

							for (Node n : this.GetChildren(this
									.FindNode(lmatch))) {

								// log.debug("match-debug"
								// + "checking = "
								// + n.GetID()
								// + " vs PatternNode:"
								// + pattern.getQ().getEdgeTarget(e)
								// .toString());

								if (n.GetAttribute() == pattern.getQ()
										.getEdgeTarget(e).attribute) {
									// System.out.println("find one:" +
									// n.GetID());
									currentMatches.add(n.GetID());
									edgeSatisfy = true;
								}
							}
						}
					}
					if (edgeSatisfy == false) {
						satisfy = false;
					}
				}

				if (satisfy == false) {
					break;
				}
				//
				// log.debug("match-debug" + "currentMatches.size = "
				// + currentMatches.size());

				lastMatches.clear();
				lastMatches.addAll(currentMatches);
			}

			if (satisfy == false) {
				continue;
			}

			else {
				xset.add(x);
			}

		}

		log.debug("pID=" + pattern.getPatternID() + " matchR using "
				+ (System.currentTimeMillis() - start) + "ms.");

		if (!xset.isEmpty()) {

			XYBitmapForPatterns.put(pattern.getPatternID(), xset);
			pattern.getXCandidates().and(xset);

		}

		return xset.toArray().length;
	}

	public int matchQ(Pattern pattern) {

		long start = System.currentTimeMillis();

		/********************** Different with MatchR Begin ************************/
		if (!this.XNotYBitmapForPatterns.containsKey(pattern.getOriginID())) {
			log.error("XBitMapKey Error.");
			return 0;
		}
		/********************** Different with MatchR End **************************/

		RoaringBitmap xset = new RoaringBitmap();

		/** Map storing edges to be mapping. HopFromX -> Edges */
		HashMap<Integer, HashSet<DefaultEdge>> oMappingEdges = new HashMap<Integer, HashSet<DefaultEdge>>();

		for (DefaultEdge e : pattern.getQ().edgeSet()) {

			/********************** Different with MatchR Begin ************************/
			if (pattern.getQ().getEdgeSource(e).nodeID == pattern.getX().nodeID
					&& pattern.getQ().getEdgeTarget(e).nodeID == pattern.getY().nodeID) {
				continue;
			}
			/********************** Different with MatchR End **************************/

			else {

				int hop = pattern.getQ().getEdgeTarget(e).hop;
				if (!oMappingEdges.containsKey(hop)) {
					oMappingEdges.put(hop, new HashSet<DefaultEdge>());
				}
				oMappingEdges.get(hop).add(e);
			}
		}

		/********************** Different with MatchR Begin ************************/
		if (oMappingEdges.size() == 0) {
			xset = XNotYBitmapForPatterns.get(pattern.getOriginID()).clone();
			return xset.toArray().length;
		}

		for (int x : XNotYBitmapForPatterns.get(pattern.getOriginID())
				.toArray()) {
			/********************** Different with MatchR End **********************/

			// log.debug("match-debug" + "current x= " + x);
			HashMap<Integer, HashSet<DefaultEdge>> mappingEdges = SerializationUtils
					.clone(oMappingEdges);

			boolean satisfy = true;

			/** Map storing edges to be mapping. PatternNodeID -> GraphNodeID */
			HashSet<Integer> lastMatches = new HashSet<Integer>();
			lastMatches.add(x);

			for (int i = 1; i <= KV.PARAMETER_B; i++) {

				// System.out.println("hop = " + i);
				if (!mappingEdges.containsKey(i)) {
					// System.out.println("checked all");
					break;
				}

				HashSet<Integer> currentMatches = new HashSet<Integer>();
				for (DefaultEdge e : mappingEdges.get(i)) {
					boolean edgeSatisfy = false;
					for (int lmatch : lastMatches) {

						if (this.FindNode(lmatch).GetAttribute() == pattern
								.getQ().getEdgeSource(e).attribute) {

							for (Node n : this.GetChildren(this
									.FindNode(lmatch))) {

								if (n.GetAttribute() == pattern.getQ()
										.getEdgeTarget(e).attribute) {

									currentMatches.add(n.GetID());
									edgeSatisfy = true;
								}
							}
						}
					}
					if (edgeSatisfy == false) {
						satisfy = false;
					}
				}

				if (satisfy == false) {
					break;
				}

				lastMatches.clear();
				lastMatches.addAll(currentMatches);
			}

			if (satisfy == false) {
				continue;
			}

			else {
				xset.add(x);
			}

		}

		log.debug("pID=" + pattern.getPatternID() + " matchR using "
				+ (System.currentTimeMillis() - start) + "ms.");

		/********************** Different with MatchR Begin ************************/
		if (!xset.isEmpty()) {
			XNotYBitmapForPatterns.put(pattern.getPatternID(), xset);
		}
		/********************** Different with MatchR End ************************/

		return xset.toArray().length;
	}

	/**
	 * Class location:
	 * ed.inf.grape.graph.Graph
	 * ed.inf.grape.graph.Node
	 * ed.inf.grape.graph.Edge
	 * ed.inf.discovery.auxiliary.SimpleNode
	 * 
	 * 
	 * @param pattern: the pattern to check
	 * @param xInPattern: x node in the pattern,
	 * @param xCandidates: x candidates in graph g (Node.ID in g).
	 * @param g: graph g.
	 * 
	 * @return x candidates satisfying pattern.
	 */
	int[] findMatch(
			DefaultDirectedGraph<ed.inf.discovery.auxiliary.SimpleNode, DefaultEdge> pattern,
			ed.inf.discovery.auxiliary.SimpleEdge xInPattern,
			int[] xCandidates, ed.inf.grape.graph.Graph g) {

		int validx[] = {};
		
		//TODO:check candidates x.
		
		return validx;
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = "
				+ this.GetNodeSize() + " | edges = " + this.GetEdgeSize();
	}

	public String getCountInfo() {
		return "X.size = " + this.X.toArray().length + " | XY.size = "
				+ this.XY.toArray().length + " | XNotY.size = "
				+ this.XNotY.toArray().length + " | YCount = " + this.YCount
				+ " | notYCount = " + this.notYCount;
	}

	public void setFreqEdge(Map<SimpleEdge, Integer> map) {
		this.freqEdge = map;
	}

	public Map<SimpleEdge, Integer> getFreqEdge() {
		return this.freqEdge;
	}

	public static void main(String[] args) {

		// Pattern [patternID=25, originID=0, partitionID=0, Q=([[NodeID:0, a=1,
		// h=0], [NodeID:1, a=2050041, h=1], [NodeID:2, a=2320003, h=1]],
		// [([NodeID:0, a=1, h=0],[NodeID:1, a=2050041, h=1]), ([NodeID:0, a=1,
		// h=0],[NodeID:2, a=2320003, h=1])]), x=[NodeID:0, a=1, h=0],
		// y=[NodeID:1, a=2050041, h=1], diameter=2]

		// For MatchR and MatchQ Test.

		Pattern p = new Pattern(0);
		p.initialXYEdge(1, 2050041);
		p.expend1Node1EdgeAsChildFromFixedNode(0, 2430010);
		// p.expend1Node1Edge(1, 201);
		// p.expend1Node1Edge(1, 1);
		// p.expend1Node1Edge(1, 1);

		System.out.println(p.toString());

		KV.PARAMETER_B = 4;

		Partition partition = IO.loadPartitionFromVEFile(0, "dataset/graph-0");
		// Partition partition = IO.loadPartitionFromVEFile(0, "dataset/test");
		partition.initWithPattern(p);
		System.out.println(partition.getCountInfo());
		System.out.println("final ret = " + partition.matchR(p));
		System.out.println("final ret = " + partition.matchQ(p));
	}

}
