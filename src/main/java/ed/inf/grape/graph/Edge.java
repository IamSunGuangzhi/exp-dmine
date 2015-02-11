package ed.inf.grape.graph;

import java.io.Serializable;

import org.jgrapht.graph.DefaultEdge;

public class Edge extends DefaultEdge implements Serializable {

	private static final long serialVersionUID = -2443804491738425451L;

	public static final String TYPE_INNER = "0";
	public static final String TYPE_OUTGOING = "1";
	public static final String TYPE_INCOMING = "2";

	public Edge() {
	}
}