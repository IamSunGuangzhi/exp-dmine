package ed.inf.grape.util;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

import ed.inf.discovery.Pattern;

public class Compute {

	static Logger log = LogManager.getLogger(Compute.class);

	/** from node label to extract edge type */
	public static int getEdgeType(int nodeLabel) {
		return nodeLabel / 1000;
	}

	public static void computeConfidence(Pattern p) {

		if (p.getXNotYCandidates().toArray().length == 0 || p.getYCount() == 0) {
			p.setConfidence(1.0);
			return;
		}

		double confidence = 0.0;

		log.debug("confidence computing " + p.getXCandidates().toArray().length
				+ " * " + p.getNotYCount() + " / "
				+ p.getXNotYCandidates().toArray().length + " * "
				+ p.getYCount());
		confidence = p.getXCandidates().toArray().length * p.getNotYCount()
				* 1.0
				/ (p.getXNotYCandidates().toArray().length * p.getYCount());
		p.setConfidence(confidence);
	}

	public static void computeUBConfidence(Pattern p) {

		if (p.getXNotYCandidates().toArray().length == 0 || p.getYCount() == 0) {
			p.setConfidence(1.0);
			return;
		}
		double confidence = 0.0;

		log.debug("confidenceup computing " + p.getSupportUB() + " * "
				+ p.getNotYCount() + " / "
				+ p.getXNotYCandidates().toArray().length + " * "
				+ p.getYCount());

		confidence = p.getSupportUB() * p.getNotYCount() * 1.0
				/ (p.getXNotYCandidates().toArray().length * p.getYCount());

		p.setConfidenceUB(confidence);
	}

	public static double computeDiff(Pattern p1, Pattern p2) {
		int inter = RoaringBitmap.and(p1.getXCandidates(), p2.getXCandidates())
				.toArray().length;
		int union = RoaringBitmap.or(p1.getXCandidates(), p2.getXCandidates())
				.toArray().length;
		return 1 - (inter * 1.0 / union);
	}

	public static double computeBF(List<Pattern> listk, double[][] diffM) {
		assert (listk.size() == KV.PARAMETER_K);
		double conf = 0.0;
		double dive = 0.0;
		for (int i = 0; i < KV.PARAMETER_K; i++) {
			conf += listk.get(i).getConfidence();
			for (int j = i + 1; j < KV.PARAMETER_K; j++) {
				dive += diffM[i][j];
			}
		}
		double bf = (1 - KV.PARAMETER_LAMBDA) * conf
				+ (2 * KV.PARAMETER_LAMBDA / (KV.PARAMETER_K - 1)) * dive;
		return bf;
	}

	public static double computeDeltaBF(List<Pattern> listk, Pattern r, int p,
			double[][] diffM) {
		assert (listk.size() == KV.PARAMETER_K && p < KV.PARAMETER_K);

		double dConf = 0.0;
		double dDive = 0.0;

		dConf = r.getConfidence() - listk.get(p).getConfidence();
		for (int i = 0; i < KV.PARAMETER_K; i++) {
			for (int j = i + 1; j < KV.PARAMETER_K; j++) {
				if (i == p) {
					dDive -= diffM[i][j];
					dDive += computeDiff(r, listk.get(j));
				} else if (j == p) {
					dDive -= diffM[i][j];
					dDive += computeDiff(r, listk.get(i));
				}
			}
		}

		double bf = (1 - KV.PARAMETER_LAMBDA) * dConf
				+ (2 * KV.PARAMETER_LAMBDA / (KV.PARAMETER_K - 1)) * dDive;
		return bf;
	}

	public static double computeDashF(Pattern r1, Pattern r2) {
		double ret = 0.0;
		ret += (1 - KV.PARAMETER_LAMBDA)
				* (r1.getConfidence() + r2.getConfidence());
		ret += (2 * KV.PARAMETER_LAMBDA) * computeDiff(r1, r2);
		return ret * 1.0 / (KV.PARAMETER_K - 1);
	}

	public static double computeBF(List<Pattern> listk) {

		assert (listk.size() <= KV.PARAMETER_K);

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
}
