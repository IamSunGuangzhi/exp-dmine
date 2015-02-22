package ed.inf.discovery;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;

import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.util.Dev;
import ed.inf.grape.util.IO;
import ed.inf.grape.util.KV;

public class DiscoveryTask {

	private int partitionID;

	/** step count */
	private int superstep;

	/** messages generated by this step */
	private List<Pattern> generatedMessages;

	static Logger log = LogManager.getLogger(DiscoveryTask.class);

	public DiscoveryTask(int partitionID) {
		this.partitionID = partitionID;
		this.generatedMessages = new LinkedList<Pattern>();
	}

	public int getPartitionID() {
		return partitionID;
	}

	public void startStep(Partition partition) {

		long start = System.currentTimeMillis();

		Pattern initPattern = new Pattern(this.partitionID);

		initPattern.initialXYEdge(KV.QUERY_X_LABEL, KV.QUERY_Y_LABEL);

		partition.initWithPattern(initPattern);

		log.debug("init count using " + (System.currentTimeMillis() - start) + "ms.");

		log.debug(partition.getCountInfo());
		log.debug(Dev.currentRuntimeState());

		List<Pattern> expandedPatterns = this.expand(partition, initPattern);
		log.debug("expanded " + expandedPatterns.size() + " patterns.");
		// TODO: automorphism check of the expendedPatterns.

		start = System.currentTimeMillis();
		for (Pattern p : expandedPatterns) {

			log.debug("pID = " + p.getPatternID() + " origin = " + p.getOriginID()
					+ ", beforeMatchR =  " + p.getXCandidates().toArray().length);

			partition.matchR(p);
			partition.matchQ(p);

			log.debug("pID = " + p.getPatternID() + ", p.xcan = "
					+ p.getXCandidates().toArray().length + ", xnotycan = "
					+ p.getXNotYCandidates().toArray().length);

			long suppStart = System.currentTimeMillis();
			int supportForNextHop = 0;
			for (int x : p.getXCandidates()) {
				if (partition.isExtendibleAtR(x, this.superstep + 1)) {
					supportForNextHop++;
				}
			}
			p.setSupportUB(supportForNextHop);
			log.debug("support upbound = " + p.getSupportUB() + ", using "
					+ (System.currentTimeMillis() - suppStart) + "ms.");

			generatedMessages.add(p);
		}
		log.debug("compute confidence using " + (System.currentTimeMillis() - start) + "ms.");
		log.debug(Dev.currentRuntimeState());

	}

	public void continuesStep(Partition partition, List<Pattern> messages) {

		log.debug("hello continue. reveived message size = " + messages.size());

		int i = 0;
		for (Pattern baseMessage : messages) {
			i++;
			log.debug("current in step " + this.superstep + " expanded " + i + "/"
					+ messages.size());

			baseMessage.resetAsLocalPattern(partition);

			List<Pattern> expandedPatterns = this.expand(partition, baseMessage);
			log.debug("expanded " + expandedPatterns.size() + " patterns.");

			long start = System.currentTimeMillis();
			for (Pattern p : expandedPatterns) {

				// log.debug(p.toString());

				log.debug("pID = " + p.getPatternID() + " origin = " + p.getOriginID()
						+ ", beforeXCan =  " + p.getXCandidates().toArray().length + ",XnotYCan= "
						+ p.getXNotYCandidates().toArray().length);

				partition.matchR(p);
				partition.matchQ(p);

				log.debug("pID = " + p.getPatternID() + ", p.xcan= "
						+ p.getXCandidates().toArray().length + ", xnotycan = "
						+ p.getXNotYCandidates().toArray().length);

				// int supportForNextHop = 0;
				// for (int x : p.getXCandidates()) {
				// if (partition.isExtendibleAtR(x, this.superstep + 1)) {
				// supportForNextHop++;
				// }
				// }
				//
				// long suppStart = System.currentTimeMillis();
				// p.setSupportUB(supportForNextHop);
				// log.debug("support upbound = " + p.getSupportUB() +
				// ", using "
				// + (System.currentTimeMillis() - suppStart) + "ms.");

				// TODO: to check whether this partition is further expandable.
				generatedMessages.add(p);
			}
			log.debug("expandtime " + (System.currentTimeMillis() - start) + "ms.");
			log.debug(Dev.currentRuntimeState());
		}

		log.debug("current step " + this.superstep + " finished.");
	}

	private List<Pattern> expand(Partition partition, Pattern origin) {

		// nodes with attribute are denote hop = -1

		List<Pattern> expandedPattern = new LinkedList<Pattern>();
		List<Pattern> expandedWithPersonNode = new LinkedList<Pattern>();

		int radiu = this.superstep;

		if (radiu == KV.PARAMETER_B) {
			log.info("radiu reaches limit, expend stoped.");
			return expandedPattern;
		}

		for (SimpleNode n : origin.getQ().vertexSet()) {
			if (n.hop == radiu && n.attribute == KV.PERSON_LABEL) {
				// only expand on radius R and person nodes.

//				Pattern np1 = new Pattern(this.partitionID, origin, false);
//				np1.expendParentFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np1);
//
//				Pattern np2 = new Pattern(this.partitionID, origin, false);
//				np2.expendLoopFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np2);

				Pattern np3 = new Pattern(this.partitionID, origin, false);
				np3.expendChildFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
				expandedWithPersonNode.add(np3);
			}

			if (n.hop == radiu && n.attribute == KV.PERSON_LABEL && KV.EXPEND_WIDTH > 1) {

//				Pattern np4 = new Pattern(this.partitionID, origin, false);
//				np4.expendParentFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				np4.expendParentFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np4);
//
//				Pattern np5 = new Pattern(this.partitionID, origin, false);
//				np5.expendLoopFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				np5.expendLoopFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np5);

				Pattern np6 = new Pattern(this.partitionID, origin, false);
				np6.expendChildFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
				np6.expendChildFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
				expandedWithPersonNode.add(np6);

//				Pattern np7 = new Pattern(this.partitionID, origin, false);
//				np7.expendParentFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				np7.expendChildFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np7);
//
//				Pattern np8 = new Pattern(this.partitionID, origin, false);
//				np8.expendLoopFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				np8.expendChildFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np8);
//
//				Pattern np9 = new Pattern(this.partitionID, origin, false);
//				np9.expendLoopFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				np9.expendParentFromFixedNodeWithAttr(n.nodeID, KV.PERSON_LABEL);
//				expandedWithPersonNode.add(np9);

			}
		}

		// if (radiu == 0) {
		// return expandedWithPersonNode;
		// }
		// int nradius = radiu +1;

		for (Pattern p : expandedWithPersonNode) {
			Map<Integer, Integer> attrs = new HashMap<Integer, Integer>();
			for (SimpleNode n : p.getQ().vertexSet()) {
				if (n.attribute != KV.PERSON_LABEL) {
					attrs.put(n.attribute, n.nodeID);
				}
			}

			for (SimpleNode n : p.getQ().vertexSet()) {
				if (n.hop == radiu && n.attribute == KV.PERSON_LABEL) {
					List<Pattern> newGensPatterns = new LinkedList<Pattern>();

					for (int attr : partition.getFreqEdgeLabels()) {
						if (attrs.keySet().contains(attr)) {
							Pattern np = new Pattern(this.partitionID, p, true);
							np.expendEdgeFromNodeToNode(n.nodeID, attrs.get(attr));
							newGensPatterns.add(np);
						} else {
							Pattern np = new Pattern(this.partitionID, p, true);
							np.expendAttrFromFixedNodeWithAttr(n.nodeID, attr);
							newGensPatterns.add(np);
						}
					}

					Iterator<Pattern> iterator = newGensPatterns.iterator();
					while (iterator.hasNext()) {
						Pattern pInGen = iterator.next();
						for (Pattern pInRet : expandedPattern) {
							if (Pattern.testSamePattern(pInRet, pInGen)) {
								iterator.remove();
							}
						}
					}
					expandedPattern.addAll(newGensPatterns);
				}
			}
		}
		return expandedPattern;
	}

	public void setSuperstep(long superstep) {
		this.superstep = (int) superstep;
	}

	public void prepareForNextCompute() {
		// TODO: reset messages
		this.generatedMessages.clear();
	}

	public List<Pattern> getMessages() {
		return this.generatedMessages;
	}

	public static void main(String[] args) {

		Pattern p = new Pattern(0);
		p.initialXYEdge(1, 2430004);
		p.expendChildFromFixedNodeWithAttr(0, 1);
		p.expendChildFromFixedNodeWithAttr(0, 1);
		// p.expend1Node1EdgeAsChildFromFixedNode(0, 2430010);

		System.out.println(p.toString());

		KV.PARAMETER_B = 4;

		Partition partition = IO.loadPartitionFromVEFile(0, "dataset/graph-0");
		// Partition partition = IO.loadPartitionFromVEFile(0, "dataset/test");
		partition.initWithPattern(p);
		System.out.println(partition.getCountInfo());

		DiscoveryTask task = new DiscoveryTask(0);
		task.superstep = 1;
		List<Pattern> ps = task.expand(partition, p);

		System.out.println("generate size:" + ps.size());

		for (Pattern pattern : ps) {
			// System.out.println(pattern);
			for (SimpleNode _v : pattern.getQ().vertexSet()) {
				StringBuffer _s = new StringBuffer();
				_s.append(_v.nodeID).append("\t").append(_v.attribute);
				for (DefaultEdge _e : pattern.getQ().outgoingEdgesOf(_v)) {
					_s.append("\t").append(pattern.getQ().getEdgeTarget(_e).nodeID);
				}
				System.out.println(_s);
				// writer.println(_s);
			}
			System.out.println("----------------------");
		}

	}
}
