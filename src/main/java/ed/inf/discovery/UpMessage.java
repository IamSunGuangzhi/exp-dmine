package ed.inf.discovery;

import java.io.Serializable;

import ed.inf.grape.graph.Graph;

public class UpMessage implements Serializable {

	/**
	 * Messages from worker to coordinator.
	 */
	private static final long serialVersionUID = -7471310937252575554L;

	private Graph Q;
	private double conf;
	private int originPartition;

	public UpMessage(Graph q, double conf, int originPartition) {
		super();
		Q = q;
		this.conf = conf;
		this.originPartition = originPartition;
	}

	public int getOriginPartition() {
		return originPartition;
	}

	@Override
	public String toString() {
		return "UpMessage [Q=" + Q + ", conf=" + conf + ", originPartition="
				+ originPartition + "]";
	}

}
