package ed.inf.grape.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

//Optimised version of http://en.wikipedia.org/wiki/Tarjan's_strongly_connected_components_algorithm
public class ConnectedComponent {

	List<Integer>[] graph;
	boolean[] visited;
	Stack<Integer> stack;
	int time;
	int[] lowlink;
	List<List<Integer>> components;

	public List<List<Integer>> scc(List<Integer>[] graph) {
		int n = graph.length;
		this.graph = graph;
		visited = new boolean[n];
		stack = new Stack<Integer>();
		time = 0;
		lowlink = new int[n];
		components = new ArrayList<List<Integer>>();

		for (int u = 0; u < n; u++)
			if (!visited[u])
				dfs(u);

		return components;
	}

	void dfs(int u) {
		lowlink[u] = time++;
		visited[u] = true;
		stack.add(u);
		boolean isComponentRoot = true;

		for (int v : graph[u]) {
			if (!visited[v])
				dfs(v);
			if (lowlink[u] > lowlink[v]) {
				lowlink[u] = lowlink[v];
				isComponentRoot = false;
			}
		}

		if (isComponentRoot) {
			List<Integer> component = new ArrayList<Integer>();
			while (true) {
				int x = stack.pop();
				component.add(x);
				lowlink[x] = Integer.MAX_VALUE;
				if (x == u)
					break;
			}
			components.add(component);
		}
	}

	// Usage example
	public static void main(String[] args) {
		List<Integer>[] g = new List[3];
		for (int i = 0; i < g.length; i++)
			g[i] = new ArrayList<Integer>();

		g[2].add(0);
		g[2].add(1);
		g[0].add(1);
		g[1].add(0);

		List<List<Integer>> components = new ConnectedComponent().scc(g);
		System.out.println(components);
	}
}