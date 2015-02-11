package ed.inf.grape.communicate;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

import ed.inf.grape.core.Worker;
import ed.inf.grape.interfaces.Result;

/**
 * Defines an interface to register remote Worker with the coordinator
 * 
 * @author Yecol
 */

public interface Worker2Coordinator extends java.rmi.Remote, Serializable {

	/**
	 * Registers remote workers with the master.
	 * 
	 * @param worker
	 *            the worker
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            the num worker threads
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */

	public Worker2Coordinator register(Worker worker, String workerID,
			int numWorkerThreads) throws RemoteException;

	/**
	 * Send a message to the Master saying that the current computation has been
	 * completed.
	 * 
	 * @param workerID
	 *            the worker id
	 */

	public void localComputeCompleted(String workerID,
			Set<String> activeWorkerIDs) throws RemoteException;

	public void sendPartialResult(String workerID,
			Map<Integer, Result> mapPartitionID2Result) throws RemoteException;

	public void shutdown() throws RemoteException;

}
