package ed.inf.grape.graph;

import java.util.Set;

public interface DirectedGraph<V> {

	public Edge getEdge(int sourceVertex, int targetVertex);

	public boolean addEdge(int sourceVertex, int targetVertex);

	public boolean containsEdge(int sourceVertex, int targetVertex);

	public boolean containsEdge(Edge e);

	public boolean addVertex(V v);

	public boolean containsVertex(V v);

	public Set<Edge> edgesOf(V vertex);

	public Edge removeEdge(V sourceVertex, V targetVertex);

	public boolean removeEdge(Edge e);

	public boolean removeVertex(V v);

	public Set<V> vertexSet();
}
