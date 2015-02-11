package ed.inf.discovery;

import java.io.Serializable;

import ed.inf.grape.graph.Graph;

public class DownMessage implements Serializable {

	/**
	 * Messages from coordinator to worker.
	 */
	private static final long serialVersionUID = -5479303760857965734L;

	private Graph Q;
	private String search;
	private int targetPartition;

	public DownMessage(Graph q, String search, int targetPartition) {
		super();
		Q = q;
		this.search = search;
		this.targetPartition = targetPartition;
	}

	public int getTargetPartition() {
		return targetPartition;
	}

	@Override
	public String toString() {
		return "DownMessage [Q=" + Q + ", search=" + search
				+ ", targetPartition=" + targetPartition + "]";
	}

}
