package ed.inf.discovery;

import java.io.Serializable;

public class UpMessage implements Serializable {

	/**
	 * Messages from worker to coordinator.
	 */
	private static final long serialVersionUID = -7471310937252575554L;

	private Pattern Q;
	private int sourcePartition;

	public Pattern getQ() {
		return Q;
	}

	public int getSourcePartition() {
		return sourcePartition;
	}

	public void setSourcePartition(int sourcePartition) {
		this.sourcePartition = sourcePartition;
	}

	public UpMessage(Pattern q, int sourcePartition) {
		super();
		Q = q;
		this.sourcePartition = sourcePartition;
	}

	@Override
	public String toString() {
		return "uMsg [QID=" + Q.getPatternID() + ", conf=" + Q.getConfidence()
				+ ", Xsize=" + Q.getXCandidates().toArray().length
				+ ", source=" + sourcePartition + "]";
	}

	public static void mergeMessage(UpMessage destination, UpMessage addToDest) {
		Pattern.add(destination.Q, addToDest.Q);
	}

}
