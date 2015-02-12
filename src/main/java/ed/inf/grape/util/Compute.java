package ed.inf.grape.util;

public class Compute {

	/** from node label to extract edge type */
	public static int getEdgeType(int nodeLabel) {
		return nodeLabel / 1000;
	}

}
