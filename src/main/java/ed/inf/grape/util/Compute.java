package ed.inf.grape.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.Pattern;
import ed.inf.discovery.auxiliary.PatternPair;

public class Compute {

	static Logger log = LogManager.getLogger(Compute.class);

	/** from node label to extract edge type */
	public static int getEdgeType(int nodeLabel) {
		return nodeLabel / 1000;
	}

	public static void computeConfidence(Pattern p, int YCount, int NotYCount) {

		if (p.getXNotYCandidates().toArray().length == 0 || YCount == 0) {
			p.setConfidence(1.0);
			return;
		}

		if (p.getNotYCount() != 84202) {
			log.debug("================================!!!!================");
			log.debug(p.toString());
		}

		double confidence = 0.0;

		log.debug("confidence computing " + p.getXCandidates().toArray().length + " * " + NotYCount
				+ " / " + p.getXNotYCandidates().toArray().length + " * " + YCount);
		confidence = p.getXCandidates().toArray().length * NotYCount * 1.0
				/ (p.getXNotYCandidates().toArray().length * YCount);
		p.setConfidence(confidence);
	}

	public static void computeUBConfidence(Pattern p, int YCount, int NotYCount) {

		if (p.getXNotYCandidates().toArray().length == 0 || YCount == 0) {
			p.setConfidence(1.0);
			return;
		}
		double confidence = 0.0;

		log.debug("support:" + p.getSupportUB());

		confidence = p.getSupportUB() * NotYCount * 1.0
				/ (p.getXNotYCandidates().toArray().length * YCount);

		p.setConfidenceUB(confidence);
	}

	public static double computeDiff(Pattern p1, Pattern p2) {
		int inter = RoaringBitmap.and(p1.getXCandidates(), p2.getXCandidates()).toArray().length;
		int union = RoaringBitmap.or(p1.getXCandidates(), p2.getXCandidates()).toArray().length;
		return 1 - (inter * 1.0 / union);
	}


	public static double computeDashF(Pattern r1, Pattern r2) {
		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA) * (r1.getConfidence() + r2.getConfidence());
		ret += (2 * KV.PARAMETER_LAMBDA) * computeDiff(r1, r2);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}

	public static double computeBF(Queue<PatternPair> pairList) {

		assert (pairList.size() <= KV.PARAMETER_K);

		List<Pattern> listk = new ArrayList<Pattern>();
		for (PatternPair pr : pairList) {
			listk.add(pr.getP1());
			listk.add(pr.getP2());
		}

		if (listk.size() <= 1) {
			return 1.0;
		}

		double conf = 0.0;
		double dive = 0.0;
		for (int i = 0; i < listk.size(); i++) {
			conf += listk.get(i).getConfidence();
			for (int j = i + 1; j < listk.size(); j++) {
				dive += computeDiff(listk.get(i), listk.get(j));
			}
		}
		double bf = (1 - KV.PARAMETER_LAMBDA) * conf
				+ (2 * KV.PARAMETER_LAMBDA / (listk.size() - 1)) * dive;
		return bf;
	}

	public static double computeLemma1(Pattern p, double maxUconfDeltaE) {

		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA) * (p.getConfidence() + maxUconfDeltaE);
		ret += (2 * KV.PARAMETER_LAMBDA);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}

	public static double computeLemma2(Pattern p, double maxUconfSigma) {

		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA) * (p.getConfidenceUB() + maxUconfSigma);
		ret += (2 * KV.PARAMETER_LAMBDA);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}
}
