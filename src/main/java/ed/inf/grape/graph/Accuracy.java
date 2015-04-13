package ed.inf.grape.graph;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.Pattern;

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

			log.info("graph partition loaded." + partition.getPartitionInfo());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return partition;
	}

	static RoaringBitmap getBitmapFromFile(final String pathToFile) {

		RoaringBitmap ret = new RoaringBitmap();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		try {

			fileInputStream = new FileInputStream(pathToFile);
			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextInt()) {
				ret.add(sc.nextInt());
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;

	}

	static ArrayList<Integer> getArrayListFromFile(final String pathToFile) {

		ArrayList<Integer> ret = new ArrayList<Integer>();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		try {

			fileInputStream = new FileInputStream(pathToFile);
			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextInt()) {
				ret.add(sc.nextInt());
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;

	}

	public Accuracy(int partitionID) {
		super();
		this.partitionID = partitionID;
		this.X = new RoaringBitmap();
		this.XY = new RoaringBitmap();
		this.XNotY = new RoaringBitmap();
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
					}
				}
			}

			if (hasXYEdgeType == true && hasXY == false) {
				XNotY.add(nodeID);
			}
		}
	}

	public RoaringBitmap getX() {
		return X;
	}

	public RoaringBitmap getXY() {
		return XY;
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
				+ " | XNotY.size = " + this.XNotY.toArray().length;
	}

	public String computePattern(int patternID) {
		final String filebase = "dataset/revision/graph4-1/";

		String ret = patternID + "\t|\t";
		DecimalFormat format = new DecimalFormat("#0.000");

		Double BFcoff = this.getXNotY().toArray().length * 1.0 / this.getXY().toArray().length;

		RoaringBitmap supp_p = getBitmapFromFile(filebase + patternID + "-r.ptn.ret");
		RoaringBitmap supp_q = getBitmapFromFile(filebase + patternID + "-q.ptn.ret");

		ArrayList<Integer> support_image = getArrayListFromFile(filebase + patternID + "-r.ptn.ret");

		Iterator<Integer> it = support_image.iterator();
		Iterator<Integer> it2;
		while (it.hasNext()) {
			int nodeID = it.next();
			RoaringBitmap neighbours = new RoaringBitmap();
			for (Node childNode : this.GetChildren(this.FindNode(nodeID))) {
				neighbours.add(childNode.GetID());
			}
			for (Node parentNode : this.GetParents(this.FindNode(nodeID))) {
				neighbours.add(parentNode.GetID());
			}

			it2 = it;
			while (it2.hasNext()) {
				int afterNodeID = it2.next();
				if (neighbours.contains(afterNodeID)) {
					it2.remove();
				}
			}

		}

		int supportRG = supp_p.toArray().length;
		int supportQG = supp_q.toArray().length;
		int supportImage = support_image.size();

		Double std = supportRG * 1.0 / supportQG;

		supp_q.and(this.XNotY);

		int supportQnqG = supp_q.toArray().length;

		Double PCA = supportRG * 1.0 / supportQnqG;
		Double BF = PCA * BFcoff;

		ret += supportRG + "\t" + supportImage + "\t" + supportQG + "\t|\t" + supportQnqG + "\t"
				+ Double.valueOf(format.format(BFcoff)) + "\t|\t"
				+ Double.valueOf(format.format(PCA)) + "\t" + Double.valueOf(format.format(BF))
				+ "\t" + supportImage + "\t" + Double.valueOf(format.format(std));

		return ret;

	}

	public void test() {
		System.out.println("pID\t|\ts(R)\tImg\ts(Q)\t|\ts(Qnq)\tcoff\t|\tPCA\tBF\tImg\tstd");
		for (int i = 0; i < 420; i++) {
			System.out.println(computePattern(i));
		}
	}

	public int getEdgeType(int attr) {
		return attr / (10000);
	}

	public static void main(String[] args) {

		Pattern p = new Pattern(0);
		p.initialXYEdge(1, 2120020);

		System.out.println(p.toString());

		Accuracy partition = Accuracy.loadPartitionFromVEFile(0, "dataset/revision/graph4-1");

		long start = System.currentTimeMillis();
		partition.initWithPattern(p);
		System.out.println("simple count using time  = " + (System.currentTimeMillis() - start)
				+ "ms");

		System.out.println(partition.getCountInfo());

		start = System.currentTimeMillis();

		partition.test();

		System.out.println("finished.");
	}
}
