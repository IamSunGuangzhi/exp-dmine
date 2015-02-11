package ed.inf.grape.graph;

import java.util.HashMap;
import java.util.Set;

public class DirectedGraphImpl<V> implements DirectedGraph<V> {

	private int[][] graphMatrix;
	private HashMap<Integer, V> graphAttribute;

	@Override
	public Edge getEdge(int sourceVertex, int targetVertex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addEdge(int sourceVertex, int targetVertex) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsEdge(int sourceVertex, int targetVertex) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsEdge(Edge e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addVertex(V v) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsVertex(V v) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<Edge> edgesOf(V vertex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Edge removeEdge(V sourceVertex, V targetVertex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean removeEdge(Edge e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeVertex(V v) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<V> vertexSet() {
		// TODO Auto-generated method stub
		return null;
	}

}
