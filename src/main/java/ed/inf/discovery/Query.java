package ed.inf.discovery;

import java.io.Serializable;

import ed.inf.grape.util.Compute;

public class Query implements Serializable {

	private static final long serialVersionUID = -95796656026667561L;

	private final int x;
	private final int y;
	private final int xyEdgeType;

	public Query(int x, int y) {
		// TODO: modify this for different data sets.
		this.x = x;
		this.y = y;
		this.xyEdgeType = Compute.getEdgeType(y);
	}

	public int getX() {
		return this.x;
	}

	public int getY() {
		return this.y;
	}

	public int getXYEdgeType() {
		return this.xyEdgeType;
	}

	@Override
	public String toString() {
		return "Query [x=" + x + ", y=" + y + ", xyEdgeType=" + xyEdgeType
				+ "]";
	}
}
