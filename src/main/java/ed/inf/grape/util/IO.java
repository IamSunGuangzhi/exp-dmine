package ed.inf.grape.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.grape.graph.Edge;
import ed.inf.grape.graph.Node;
import ed.inf.grape.graph.Partition;

public class IO {

	static Logger log = LogManager.getLogger(IO.class);

	static public Partition loadPartitionFromVEFile(final int partitionID,
			final String partitionFilename) {
		/**
		 * Load partition from file. Each partition consists two files: 1.
		 * partitionName.v: vertexID vertexLabel 2. partitionName.e:
		 * edgeSource-edgeTarget
		 * */

		log.info("loading partition " + partitionFilename
				+ " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		Partition partition = new Partition(partitionID);

		/** load vertices */
		try {
			fileInputStream = new FileInputStream(partitionFilename + ".v");

			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				String[] elements = sc.nextLine().split("\t");
				int vid = Integer.parseInt(elements[0].trim());
				int vlabel = Integer.parseInt(elements[1].trim());

				Node n = new Node(vid, vlabel);
				partition.InsNode(n);
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			log.debug("load vertex finished.");

			/** load edges */
			fileInputStream = new FileInputStream(partitionFilename + ".e");
			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				String[] elements = sc.nextLine().split("\t");

				Node source = partition.FindNode(Integer.parseInt(elements[0]
						.trim()));
				Node target = partition.FindNode(Integer.parseInt(elements[1]
						.trim()));

				partition.InsEdge(source, target);
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			log.info("graph partition loaded." + partition.getPartitionInfo()
					+ ", using " + (System.currentTimeMillis() - startTime)
					+ " ms");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return partition;
	}

	static public Map<Integer, Integer> loadInt2IntMapFromFile(String filename)
			throws IOException {

		HashMap<Integer, Integer> retMap = new HashMap<Integer, Integer>();

		log.info("loading map " + filename + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		fileInputStream = new FileInputStream(filename);
		sc = new Scanner(fileInputStream, "UTF-8");
		while (sc.hasNextInt()) {

			int key = sc.nextInt();
			int value = sc.nextInt();
			retMap.put(key, value);

		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info(filename + " loaded to map. with size =  " + retMap.size()
				+ ", using " + (System.currentTimeMillis() - startTime) + " ms");

		return retMap;
	}

	static public Map<String, Integer> loadString2IntMapFromFile(String filename)
			throws IOException {

		HashMap<String, Integer> retMap = new HashMap<String, Integer>();

		log.info("loading map " + filename + " with stream scanner.");

		long startTime = System.currentTimeMillis();

		FileInputStream fileInputStream = null;
		Scanner sc = null;

		fileInputStream = new FileInputStream(filename);
		sc = new Scanner(fileInputStream, "UTF-8");

		int ln = 0;

		while (sc.hasNext()) {

			if (ln % 100000 == 0) {
				log.info("read line " + ln);
			}
			String key = sc.next();
			int value = sc.nextInt();
			retMap.put(key, value);
			ln++;
		}

		if (fileInputStream != null) {
			fileInputStream.close();
		}
		if (sc != null) {
			sc.close();
		}

		log.info(filename + " loaded to map. with size =  " + retMap.size()
				+ ", using " + (System.currentTimeMillis() - startTime) + " ms");

		return retMap;
	}

	static public <K, V> void writeMapToFile(Map<K, V> map, String filename) {

		log.info("writing map to " + filename + "");

		long startTime = System.currentTimeMillis();

		PrintWriter writer;
		try {
			writer = new PrintWriter(filename, "UTF-8");

			for (Entry<K, V> entry : map.entrySet()) {
				writer.println(entry.getKey() + "\t" + entry.getValue());
			}

			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		log.info(filename + " write to file. map size =  " + map.size()
				+ ", using " + (System.currentTimeMillis() - startTime) + " ms");
	}

	public static boolean serialize(String filePath, Object obj) {
		boolean serialized = false;
		FileOutputStream fileOutputStream = null;
		ObjectOutputStream objectOutputStream = null;
		try {
			fileOutputStream = new FileOutputStream(filePath);
			objectOutputStream = new ObjectOutputStream(fileOutputStream);
			objectOutputStream.writeObject(obj);
			objectOutputStream.flush();
			objectOutputStream.close();
			serialized = true;
		} catch (IOException e) {
			e.printStackTrace();
			serialized = false;
		} finally {
			try {
				fileOutputStream.close();
				objectOutputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
				serialized = false;
			}
		}
		return serialized;
	}

	/**
	 * Deserialize the object from the file specified by the file path.
	 * 
	 * @param filePath
	 *            the file path
	 * @return obj the deserialized object
	 */
	public static Object deserialize(String filePath) {
		FileInputStream fileInputStream = null;
		ObjectInputStream objectInputStream = null;
		Object obj = null;
		try {
			fileInputStream = new FileInputStream(filePath);
			objectInputStream = new ObjectInputStream(fileInputStream);
			obj = objectInputStream.readObject();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				fileInputStream.close();
				objectInputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return obj;
	}
}
