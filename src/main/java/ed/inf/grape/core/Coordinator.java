package ed.inf.grape.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.discovery.DownMessage;
import ed.inf.discovery.Pattern;
import ed.inf.grape.client.Command;
import ed.inf.grape.communicate.Client2Coordinator;
import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.communicate.WorkerProxy;
import ed.inf.grape.util.Compute;
import ed.inf.grape.util.Dev;
import ed.inf.grape.util.KV;

/**
 * The Class Coordinator.
 * 
 * @author yecol
 */
public class Coordinator extends UnicastRemoteObject implements
		Worker2Coordinator, Client2Coordinator {

	private static final long serialVersionUID = 7264167926318903124L;

	/** The total number of worker threads. */
	private static AtomicInteger totalWorkerThreads = new AtomicInteger(0);

	/** The workerID to WorkerProxy map. */
	private Map<String, WorkerProxy> workerProxyMap = new ConcurrentHashMap<String, WorkerProxy>();

	/** The workerID to Worker map. **/
	private Map<String, Worker> workerMap = new HashMap<String, Worker>();

	/** The partitionID to workerID map. **/
	private Map<Integer, String> partitionWorkerMap;

	/** Set of Workers maintained for acknowledgement. */
	private Set<String> workerAcknowledgementSet = new HashSet<String>();

	/** Set of workers who will be active in the next super step. */
	private Set<String> activeWorkerSet = new HashSet<String>();

	private Map<Integer, List<Pattern>> receivedMessages = new HashMap<Integer, List<Pattern>>();

	/** Merged Message **/
	private List<Pattern> mergedMessages = new LinkedList<Pattern>();

	private List<Pattern> listK = new ArrayList<Pattern>();

	private double[][] diffM = new double[KV.PARAMETER_K][KV.PARAMETER_K];
	private double bf;

	/** The start time. */
	long startTime;

	long superstep = 0;

	static Logger log = LogManager.getLogger(Coordinator.class);

	/**
	 * Instantiates a new coordinator.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 * @throws PropertyNotFoundException
	 *             the property not found exception
	 */
	public Coordinator() throws RemoteException {
		super();

	}

	/**
	 * Gets the active worker set.
	 * 
	 * @return the active worker set
	 */
	public Set<String> getActiveWorkerSet() {
		return activeWorkerSet;
	}

	/**
	 * Sets the active worker set.
	 * 
	 * @param activeWorkerSet
	 *            the new active worker set
	 */
	public void setActiveWorkerSet(Set<String> activeWorkerSet) {
		this.activeWorkerSet = activeWorkerSet;
	}

	/**
	 * Registers the worker computation nodes with the master.
	 * 
	 * @param worker
	 *            Represents the {@link WorkerSyncImpl.WorkerImpl Worker}
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            Represents the number of worker threads available in the
	 *            worker computation node
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public Worker2Coordinator register(Worker worker, String workerID,
			int numWorkerThreads) throws RemoteException {

		log.debug("Coordinator: Register");
		totalWorkerThreads.getAndAdd(numWorkerThreads);
		WorkerProxy workerProxy = new WorkerProxy(worker, workerID,
				numWorkerThreads, this);
		workerProxyMap.put(workerID, workerProxy);
		workerMap.put(workerID, worker);
		return (Worker2Coordinator) UnicastRemoteObject.exportObject(
				workerProxy, 0);
	}

	/**
	 * Gets the worker proxy map info.
	 * 
	 * @return Returns the worker proxy map info
	 */
	public Map<String, WorkerProxy> getWorkerProxyMap() {
		return workerProxyMap;
	}

	/**
	 * Send worker partition info.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void sendWorkerPartitionInfo() throws RemoteException {
		log.debug("Coordinator: sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setWorkerPartitionInfo(null, partitionWorkerMap,
					workerMap);
		}
	}

	public void sendQuery(String query) throws RemoteException {
		log.debug("Coordinator: sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setQuery(query);
		}
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.setSecurityManager(new RMISecurityManager());
		Coordinator coordinator;
		try {
			coordinator = new Coordinator();
			Registry registry = LocateRegistry.createRegistry(KV.RMI_PORT);
			registry.rebind(KV.COORDINATOR_SERVICE_NAME, coordinator);
			String resultFolder = (new SimpleDateFormat("yyyyMMdd-hh-mm-ss"))
					.format(new Date());
			KV.RESULT_DIR = KV.OUTPUT_DIR + resultFolder;
			(new File(KV.RESULT_DIR)).mkdir();
			log.info("Coordinator instance is bound to " + KV.RMI_PORT
					+ " and ready.");
		} catch (RemoteException e) {
			Coordinator.log.error(e);
			e.printStackTrace();
		}
	}

	/**
	 * Halts all the workers and prints the final solution.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		// healthManager.exit();
		log.info("Master: halt");
		log.debug("Worker Proxy Map " + workerProxyMap);

		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.halt();
		}

		// healthManager.exit();
		long endTime = System.currentTimeMillis();
		log.info("Time taken: " + (endTime - startTime) + " ms");
		// Restore the system back to its initial state
		restoreInitialState();
	}

	/**
	 * Restore initial state of the system.
	 */
	private void restoreInitialState() {
		this.activeWorkerSet.clear();
		this.workerAcknowledgementSet.clear();
		this.partitionWorkerMap.clear();
		this.superstep = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */

	/**
	 * Removes the worker.
	 * 
	 * @param workerID
	 *            the worker id
	 */
	public void removeWorker(String workerID) {
		workerProxyMap.remove(workerID);
		workerMap.remove(workerID);
	}

	/**
	 * Gets the partition worker map.
	 * 
	 * @return the partition worker map
	 */
	public Map<Integer, String> getPartitionWorkerMap() {
		return partitionWorkerMap;
	}

	/**
	 * Sets the partition worker map.
	 * 
	 * @param partitionWorkerMap
	 *            the partition worker map
	 */
	public void setPartitionWorkerMap(Map<Integer, String> partitionWorkerMap) {
		this.partitionWorkerMap = partitionWorkerMap;
	}

	/**
	 * Defines a deployment convenience to stop each registered.
	 * 
	 * @throws RemoteException
	 *             the remote exception {@link system.Worker Worker} and then
	 *             stops itself.
	 */

	@Override
	public void shutdown() throws RemoteException {
		// if (healthManager != null)
		// healthManager.exit();
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			try {
				workerProxy.shutdown();
			} catch (Exception e) {
				continue;
			}
		}
		java.util.Date date = new java.util.Date();
		log.info("Master goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	public void loadGraph(String graphFilename) throws RemoteException {

		log.info("load Graph = " + graphFilename);

		assignDistributedPartitions();
		sendWorkerPartitionInfo();
	}

	@Override
	public void putTask(String query) throws RemoteException {

		log.info(query);

		/** initiate local compute tasks. */
		sendQuery(query);

		/** begin to compute. */
		nextLocalCompute();
		startTime = System.currentTimeMillis();
	}

	public void assignDistributedPartitions() {

		/**
		 * Graph file has been partitioned, and the partitioned graph have been
		 * distributed to workers.
		 * 
		 * */

		partitionWorkerMap = new HashMap<Integer, String>();

		int currentPartitionID = 0;

		// Assign partitions to workers in the ratio of the number of worker
		// threads that each worker has.
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {

			WorkerProxy workerProxy = entry.getValue();

			// Compute the number of partitions to assign
			int numThreads = workerProxy.getNumThreads();
			double ratio = ((double) (numThreads)) / totalWorkerThreads.get();
			log.info("Worker " + workerProxy.getWorkerID());
			log.info("ThreadNum = " + numThreads + ", Ratio: " + ratio);
			int numPartitionsToAssign = (int) (ratio * KV.PARTITION_COUNT);
			log.info("numPartitionsToAssign: " + numPartitionsToAssign);

			List<Integer> workerPartitionIDs = new ArrayList<Integer>();
			for (int i = 0; i < numPartitionsToAssign; i++) {
				if (currentPartitionID < KV.PARTITION_COUNT) {
					activeWorkerSet.add(entry.getKey());
					log.info("Adding partition  " + currentPartitionID
							+ " to worker " + workerProxy.getWorkerID());
					workerPartitionIDs.add(currentPartitionID);
					partitionWorkerMap.put(currentPartitionID,
							workerProxy.getWorkerID());
					currentPartitionID++;
				}
			}
			workerProxy.addPartitionIDList(workerPartitionIDs);
		}

		if (currentPartitionID < KV.PARTITION_COUNT) {
			// Add the remaining partitions (if any) in a round-robin fashion.
			Iterator<Map.Entry<String, WorkerProxy>> workerMapIter = workerProxyMap
					.entrySet().iterator();

			while (currentPartitionID != KV.PARTITION_COUNT) {
				// If the remaining partitions is greater than the number of the
				// workers, start iterating from the beginning again.
				if (!workerMapIter.hasNext()) {
					workerMapIter = workerProxyMap.entrySet().iterator();
				}

				WorkerProxy workerProxy = workerMapIter.next().getValue();

				activeWorkerSet.add(workerProxy.getWorkerID());
				log.info("Adding partition  " + currentPartitionID
						+ " to worker " + workerProxy.getWorkerID());
				partitionWorkerMap.put(currentPartitionID,
						workerProxy.getWorkerID());
				workerProxy.addPartitionID(currentPartitionID);

				currentPartitionID++;
			}
		}
	}

	/**
	 * Start super step.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public synchronized void nextLocalCompute() throws RemoteException {

		log.info("Coordinator: next local compute. superstep = " + superstep);

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).workerRunNextStep(superstep);
		}
		this.activeWorkerSet.clear();
	}

	public void prepareForNextStep() {
		this.receivedMessages.clear();
		this.mergedMessages.clear();
	}

	public void finishDiscovery() {
		// TODO: output final result
		long mineTime = System.currentTimeMillis() - startTime;

		log.info("finishedDiscovery, time = " + mineTime * 1.0 / 1000 + "s.");
	}

	private void increamentalDiverfy(Pattern m) {

		if (listK.size() < KV.PARAMETER_K) {

			// <k, add m.
			listK.add(m);
			if (listK.size() == KV.PARAMETER_K) {

				// init divM values
				for (int i = 0; i < KV.PARAMETER_K; i++) {
					for (int j = i + 1; j < KV.PARAMETER_K; j++) {
						diffM[i][j] = Compute.computeDiff(listK.get(i),
								listK.get(j));
					}
				}

				for (int i = 0; i < KV.PARAMETER_K; i++) {
					for (int j = 0; j < KV.PARAMETER_K; j++) {
						System.out.print(diffM[i][j] + "\t");
					}
					System.out.print("\n");
				}

				this.bf = Compute.computeBF(listK, diffM);
				log.info("init BF = " + this.bf);
			}
		}

		else if (listK.size() == KV.PARAMETER_K) {
			double max = -Double.MAX_VALUE;
			int position = -1;
			for (int i = 0; i < KV.PARAMETER_K; i++) {
				double delta = Compute.computeDeltaBF(listK, m, i, diffM);
				if (delta > max) {
					max = delta;
					position = i;
				}
			}
			if (max > 0) {
				listK.remove(position);
				listK.add(position, m);

				log.info("replace " + m + "th pattern in topk, due to delta = "
						+ max);

				for (int i = 0; i < KV.PARAMETER_K; i++) {
					for (int j = i + 1; j < KV.PARAMETER_K; j++) {
						if (i == position || j == position) {
							diffM[i][j] = Compute.computeDiff(listK.get(i),
									listK.get(j));
						}
					}
				}

				this.bf = Compute.computeBF(listK, diffM);
				log.info("replaced BF = " + this.bf);
			}
		}

	}

	private void generateTopK() {
		log.debug("begin generate topk with " + this.mergedMessages.size()
				+ " mergedMsg.");
		long start = System.currentTimeMillis();
		for (Pattern m : this.mergedMessages) {
			increamentalDiverfy(m);
		}
		log.debug("generate topk time = "
				+ (System.currentTimeMillis() - start) + "ms");
	}

	private void assembleMessages() {
		// TODO: update activeWorkerSet
		log.debug("begin assemble" + this.receivedMessages.size());
		int isotesttime = 0;
		boolean firstSetFlag = true;
		long start = System.currentTimeMillis();
		for (int curPartitionID : this.receivedMessages.keySet()) {

			this.printMessageList(this.receivedMessages.get(curPartitionID));

			if (firstSetFlag) {
				this.mergedMessages.addAll(this.receivedMessages
						.get(curPartitionID));
				firstSetFlag = false;
			} else {
				for (Pattern message : this.receivedMessages
						.get(curPartitionID)) {
					boolean findFlag = false;
					for (Pattern assembledMessage : this.mergedMessages) {
						isotesttime++;
						if (Pattern.testSamePattern(assembledMessage, message)) {
							Pattern.add(assembledMessage, message);
							findFlag = true;
							break;
						}
					}
					if (!findFlag) {
						this.mergedMessages.add(message);
					}
				}
			}
		}

		log.debug("assemble time = " + (System.currentTimeMillis() - start)
				+ "ms, do iso times = " + isotesttime);
		// this.printMessageList(mergedMessages);
		// for (UpMessage message : this.receivedMessages) {
		// log.debug(message.toString());
		// }
		// TODO: send message downwards and trigger next local compute.
	}

	public synchronized void receiveMessages(String workerID,
			List<Pattern> upMessages) {
		log.info("Coordinator received message from worker " + workerID
				+ " message-size: " + upMessages.size());

		log.debug(Dev.currentRuntimeState());

		for (Pattern m : upMessages) {
			if (!receivedMessages.containsKey(m.getPartitionID())) {
				receivedMessages.put(m.getPartitionID(),
						new LinkedList<Pattern>());
			}
			receivedMessages.get(m.getPartitionID()).add(m);
		}

		// this.receivedMessages.addAll(upMessages);
		this.workerAcknowledgementSet.remove(workerID);

		log.info("Coordinator received message" + workerID + " down.");
		log.debug(Dev.currentRuntimeState());

		if (this.workerAcknowledgementSet.size() == 0) {

			if (this.receivedMessages.size() == 0) {

				// workers didn't expanded anything.
				this.writeTopKToFile();
				this.finishDiscovery();
			}

			else {

				// receive expanded GPARs

				this.assembleMessages();
				this.generateTopK();
				this.writeTopKToFile();

				// for next expand.

				this.prepareForNextStep();
				this.superstep++;

				// TODO: test if trigger all the workers. if null then finished.
				this.activeWorkerSet.addAll(this.workerMap.keySet());

				try {
					nextLocalCompute();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				// finishDiscovery();
			}
		}

	}

	@Override
	public void preProcess() throws RemoteException {
		this.loadGraph(KV.GRAPH_FILE_PATH_PREFIX);
	}

	@Override
	public void postProcess() throws RemoteException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendMessageWorker2Coordinator(String workerID,
			List<Pattern> messages) throws RemoteException {
	}

	@Override
	public void sendMessageCoordinator2Worker(String workerID,
			List<DownMessage> messages) throws RemoteException {
	}

	public void printMessageList(List<Pattern> list) {
		log.debug("message list size = " + list.size());
		for (Pattern um : list) {
			log.debug(um.toString());
		}
	}

	public void writeTopKToFile() {

		String resultFile = KV.RESULT_DIR + "/" + this.superstep + ".dat";

		PrintWriter writer;
		try {

			writer = new PrintWriter(resultFile);
			writer.println("======================");
			writer.println("round = " + this.superstep + ", bf = " + this.bf);
			writer.println("time = " + (System.currentTimeMillis() - startTime)
					* 1.0 / 1000 + "s.");

			for (Pattern m : this.listK) {
				writer.println(m);
				writer.println("----------------------");
			}
			writer.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
