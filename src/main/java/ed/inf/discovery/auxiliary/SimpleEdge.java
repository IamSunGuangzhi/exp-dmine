package ed.inf.discovery.auxiliary;

public class SimpleEdge {
	public int fnode;
	public int tnode;

	public SimpleEdge(String edge) {
		String[] nodes = edge.split("-");
		this.fnode = Integer.parseInt(nodes[0]);
		this.tnode = Integer.parseInt(nodes[1]);
	}

	public SimpleEdge(int fnode, int tnode) {
		this.fnode = fnode;
		this.tnode = tnode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + fnode;
		result = prime * result + tnode;
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
		SimpleEdge other = (SimpleEdge) obj;
		if (fnode != other.fnode)
			return false;
		if (tnode != other.tnode)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return fnode + "-" + tnode;
	}

}
