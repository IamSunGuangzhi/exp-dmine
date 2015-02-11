package ed.inf.grape.core;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

import ed.inf.discovery.DownMessage;
import ed.inf.discovery.Query;
import ed.inf.grape.communicate.Worker2Coordinator;

public interface Worker extends Remote {

	public String getWorkerID() throws RemoteException;

	public int getNumThreads() throws RemoteException;

	public void setCoordinatorProxy(Worker2Coordinator coordinatorProxy)
			throws RemoteException;

	public void setQuery(Query query) throws RemoteException;

	public void addPartitionID(int partitionID) throws RemoteException;

	public void addPartitionIDList(List<Integer> workerPartitionIDs)
			throws RemoteException;

	public void setWorkerPartitionInfo(int totalPartitionsAssigned,
			Map<Integer, Integer> mapVertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId,
			Map<String, Worker> mapWorkerIdToWorker) throws RemoteException;

	public void halt() throws RemoteException;

	public void receiveMessage(List<DownMessage> incomingMessages)
			throws RemoteException;

	public void nextStep(long superstep) throws RemoteException;

	public void processPartialResult() throws RemoteException;

	public void shutdown() throws RemoteException;

}