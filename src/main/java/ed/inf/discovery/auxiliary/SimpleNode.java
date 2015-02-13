package ed.inf.discovery.auxiliary;

import java.io.Serializable;

public class SimpleNode implements Serializable {

	public int nodeID;
	public int attribute;

	public SimpleNode(int nodeID, int attribute) {
		super();
		this.nodeID = nodeID;
		this.attribute = attribute;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + attribute;
		result = prime * result + nodeID;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimpleNode other = (SimpleNode) obj;
		if (attribute != other.attribute)
			return false;
		if (nodeID != other.nodeID)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Node ID=" + nodeID + ", attri=" + attribute + "]";
	}

}
