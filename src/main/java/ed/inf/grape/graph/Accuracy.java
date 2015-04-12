package ed.inf.grape.graph;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.Pattern;
import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.discovery.auxiliary.function;

/**
 * Data structure of partition, including a graph fragment and vertices with
 * crossing edges.
 * 
 * @author yecol
 *
 */

public class Accuracy extends Graph implements Serializable {

	private static final long serialVersionUID = -4757004627010733180L;

	private int partitionID;

	/** Node set in BF functions */

	private RoaringBitmap X;

	private RoaringBitmap XY;
	private RoaringBitmap XNotY;

	private int YCount = 0;
	private int notYCount = 0;

	private Set<Integer> freqEdgeLabels;

	private function ISOHelper;

	/** pattern and its valid Xs */
	// private HashMap<Integer, RoaringBitmap> XYBitmapForPatterns;
	// private HashMap<Integer, RoaringBitmap> XNotYBitmapForPatterns;

	/** Statistics of current partition */

	static Logger log = LogManager.getLogger(Accuracy.class);

	static public Accuracy loadPartitionFromVEFile(final int partitionID,
			final String partitionFilename) {
		/**
		 * Load partition from file. Each partition consists two files: 1.
		 * partitionName.v: vertexID vertexLabel 2. partitionName.e:
		 * edgeSource-edgeTarget
		 * */

		log.info("loading partition " + partitionFilename + " with stream scanner.");

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		Accuracy partition = new Accuracy(partitionID);

		/** load vertices */
		try {
			fileInputStream = new FileInputStream(partitionFilename + ".v");

			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				String[] elements = sc.nextLine().split("\t");
				int vid = Integer.parseInt(elements[0].trim());
				int vlabel = Integer.parseInt(elements[1].trim());

				Node n = new Node(vid, vlabel);
				partition.InsNode(n);
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			log.debug("load vertex finished.");

			/** load edges */
			fileInputStream = new FileInputStream(partitionFilename + ".e");
			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				String[] elements = sc.nextLine().split("\t");

				Node source = partition.FindNode(Integer.parseInt(elements[0].trim()));
				Node target = partition.FindNode(Integer.parseInt(elements[1].trim()));

				partition.InsEdge(source, target);
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			// partition.setFreqEdgeLabels(IO.loadFrequentEdgeSetFromFile(KV.FREQUENT_EDGE));

			log.info("graph partition loaded." + partition.getPartitionInfo());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return partition;
	}

	static ArrayList<Pattern> loadGammaFromFile(final String pathToFile) {

		ArrayList<Pattern> ret = new ArrayList<Pattern>();

		try {

			FileReader reader = new FileReader(pathToFile);
			BufferedReader br = new BufferedReader(reader);

			String _line = null;
			Pattern _pattern = null;
			boolean _flag = true;

			HashMap<Integer, SimpleNode> _vertexs = new HashMap<Integer, SimpleNode>();
			HashMap<Integer, ArrayList<Integer>> _edges = new HashMap<Integer, ArrayList<Integer>>();
			int _x = 0, _y = 0;

			while ((_line = br.readLine()) != null) {

				if (_line.startsWith("===")) {

					_flag = true;

					if (_pattern != null) {

						for (Integer _source : _edges.keySet()) {
							for (Integer _target : _edges.get(_source)) {
								_pattern.getQ().addEdge(_vertexs.get(_source),
										_vertexs.get(_target));
							}
						}

//						_pattern.setX(_vertexs.get(_x));
//						_pattern.setY(_vertexs.get(_y));
						_pattern.setXY(_vertexs.get(_x), _vertexs.get(_y));

						ret.add(_pattern);
					}

					_vertexs.clear();
					_edges.clear();

					_pattern = new Pattern(0);
					continue;
				}

				String[] elements = _line.split("\t");

				if (_flag == true) {
					// represents x and y
					_x = Integer.parseInt(elements[0]);
					_y = Integer.parseInt(elements[1]);

					_flag = false;
					continue;
				}

				if (elements.length < 2) {
					continue;
				} else {

					SimpleNode v = new SimpleNode(Integer.parseInt(elements[0]),
							Integer.parseInt(elements[1]), 0);
					_pattern.getQ().addVertex(v);
					// _pattern.updateMaxIndex(v.nodeID);
					_vertexs.put(Integer.parseInt(elements[0]), v);
				}

				if (elements.length > 2) {
					if (!_edges.containsKey(Integer.parseInt(elements[0]))) {
						_edges.put(Integer.parseInt(elements[0]), new ArrayList<Integer>());
					}

					for (int i = 2; i < elements.length; i++) {
						_edges.get(Integer.parseInt(elements[0]))
								.add(Integer.parseInt(elements[i]));
					}
				}
			}

			br.close();
			reader.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("loadGraphFromFile loaded. with " + ret.size() + " patterns.");

		for (Pattern p : ret) {
			System.out.println(p + " with v:" + p.getQ().vertexSet().size() + " e:"
					+ p.getQ().edgeSet().size());
		}

		return ret;

	}

	public Accuracy(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.X = new RoaringBitmap();
		this.XY = new RoaringBitmap();
		this.XNotY = new RoaringBitmap();

		this.freqEdgeLabels = new HashSet<Integer>();
		this.ISOHelper = new function();
		// this.XYBitmapForPatterns = new HashMap<Integer, RoaringBitmap>();
		// this.XNotYBitmapForPatterns = new HashMap<Integer, RoaringBitmap>();
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
		for (Node node : this.GetNodeSet().values()) {
			if (node.GetAttribute() == pattern.getX().attribute) {
				X.add(node.GetID());
			}
		}

		System.out.println("Node X size = " + X.toArray().length);

		for (int nodeID : this.X) {

			/** count x with edge xy and xnoty but other */
			/** a x with edge xy count in xy but not xnoty */

			boolean hasXYEdgeType = false;
			boolean hasXY = false;

			for (Node childNode : this.GetChildren(this.FindNode(nodeID))) {
				if (this.getEdgeType(childNode.GetAttribute()) == this
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

		pattern.setXCandidates(XY);
		pattern.setXnotYCandidates(XNotY);
		pattern.setSupportUB(XY.toArray().length);
		pattern.setYCount(YCount);
		pattern.setNotYCount(notYCount);
	}

	public int getYCount() {
		return YCount;
	}

	public int getNotYCount() {
		return notYCount;
	}

	public RoaringBitmap getX() {
		return X;
	}

	public RoaringBitmap getXNotY() {
		return XNotY;
	}

	public String getPartitionInfo() {
		return "pID = " + this.partitionID + " | vertices = " + this.GetNodeSize() + " | edges = "
				+ this.GetEdgeSize();
	}

	public String getCountInfo() {
		return "X.size = " + this.X.toArray().length + " | XY.size = " + this.XY.toArray().length
				+ " | XNotY.size = " + this.XNotY.toArray().length + " | YCount = " + this.YCount
				+ " | notYCount = " + this.notYCount;
	}

	public void setFreqEdgeLabels(Set<Integer> freqEdgeSet) {
		this.freqEdgeLabels = freqEdgeSet;
	}

	public Set<Integer> getFreqEdgeLabels() {
		return this.freqEdgeLabels;
	}

	public int getEdgeType(int attr) {
		return attr / (1000000);
	}

	// public int matchVF2R(Pattern p) {
	//
	// long start = System.currentTimeMillis();
	//
	// System.out.println("we begin match R");
	//
	// HashSet<Integer> validX = this.iso_helper.IsoCheck(p.toPGraph(), 0,
	// p.getXCandidates()
	// .toArray(), this);
	//
	// System.out.println("validx.size = " + validX.size());
	//
	// RoaringBitmap xset = new RoaringBitmap();
	// for (int x : validX) {
	// xset.add(x);
	// }
	//
	// p.getXCandidates().and(xset);
	// log.debug("pID=" + p.getPatternID() + " matchQ using "
	// + (System.currentTimeMillis() - start) + "ms.");
	//
	// return xset.toArray().length;
	// }
	//
	// public int matchVF2Q(Pattern p) {
	// long start = System.currentTimeMillis();
	//
	// HashSet<Integer> validX = this.iso_helper.IsoCheck(p.toQGraph(), 0,
	// p.getXCandidates()
	// .toArray(), this);
	//
	// RoaringBitmap xset = new RoaringBitmap();
	// for (int x : validX) {
	// xset.add(x);
	// }
	//
	// p.getXCandidates().and(xset);
	// log.debug("pID=" + p.getPatternID() + " matchQ using "
	// + (System.currentTimeMillis() - start) + "ms.");
	//
	// return xset.toArray().length;
	//
	// }

	public static void main(String[] args) {

		// Pattern [patternID=25, originID=0, partitionID=0, Q=([[NodeID:0, a=1,
		// h=0], [NodeID:1, a=2050041, h=1], [NodeID:2, a=2320003, h=1]],
		// [([NodeID:0, a=1, h=0],[NodeID:1, a=2050041, h=1]), ([NodeID:0, a=1,
		// h=0],[NodeID:2, a=2320003, h=1])]), x=[NodeID:0, a=1, h=0],
		// y=[NodeID:1, a=2050041, h=1], diameter=2]

		// For MatchR and MatchQ Test.

		Pattern p = new Pattern(0);
		p.initialXYEdge(1, 2120027);

		System.out.println(p.toString());

		Accuracy partition = Accuracy.loadPartitionFromVEFile(0, "dataset/pokec/graph-0");
		
		ArrayList<Pattern> patterns = Accuracy.loadGammaFromFile("dataset/pokec/gamma.dat");
		
		long start = System.currentTimeMillis();
		partition.initWithPattern(p);
		System.out.println("simple count using time  = " + (System.currentTimeMillis() - start)
				+ "ms");

		System.out.println(partition.getCountInfo());

		function f = new function();
		// int[] v_index_set = { 1, 2, 10, 15, 17 };
		start = System.currentTimeMillis();

		// p.getQ().Display();
		HashSet<Integer> set = f.IsoCheck(p.toPGraph(), 0, partition.X.toArray(), partition);

		System.out.println("VF2 ISO check result size = " + set.size());
		System.out.println("VF2 using time  = " + (System.currentTimeMillis() - start) + "ms");
		
		for(Pattern p4test : patterns){
			start = System.currentTimeMillis();
			p4test.toPGraph().Display();
			HashSet<Integer> temp = f.IsoCheck(p4test.toPGraph(), 0, partition.XY.toArray(), partition);
			System.out.println("VF2 ISO check result size = " + temp.size());
			System.out.println("VF2 using time  = " + (System.currentTimeMillis() - start) + "ms");
		}

		System.out.println("finished.");
	}
}
