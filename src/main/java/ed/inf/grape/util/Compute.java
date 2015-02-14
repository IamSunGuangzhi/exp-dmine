package ed.inf.grape.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Compute {

	static Logger log = LogManager.getLogger(Compute.class);

	/** from node label to extract edge type */
	public static int getEdgeType(int nodeLabel) {
		return nodeLabel / 1000;
	}

	public static double computeConfidence(int r, int q, int y, int ny) {
		if (q == 0 || ny == 0) {
			log.error("compute confidence error. q=" + q + ", ny=" + ny);
			return 0;
		}
		return r * y * 1.0 / (q * ny);
	}

}
