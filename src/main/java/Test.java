import java.util.Vector;

import Query.executor;
import ed.inf.grape.graph.Graph;
import ed.inf.grape.graph.function;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		executor exe = new executor();
		Graph p = exe.patternGen();
		Graph g = exe.graphGen();

		function f = new function();
		int[] v_index_set = { 1, 2, 3, 4 };
		Vector<Integer> set = f.IsoCheck(p, 2, v_index_set, g);

		// Vector<Integer> result = f.IsoCheck(p, 0,
		// partition.X.toArray(), (Graph) partition);
		//
		System.out.println("size = " + set.size());
		for (int i : set) {
			System.out.println(i);
		}

	}

}
