package ed.inf.grape.client;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import ed.inf.grape.communicate.Client2Coordinator;
import ed.inf.grape.util.KV;

public class Command {

	public static void main(String[] args) throws RemoteException,
			NotBoundException, MalformedURLException, ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		if (args.length < 3) {
			System.out.println("3 args: coordinator k b");
			System.exit(0);
		}

		String coordinatorMachineName = args[0];
		KV.PARAMETER_K = Integer.parseInt(args[1]);
		KV.PARAMETER_B = Integer.parseInt(args[2]);

		String coordinatorURL = "//" + coordinatorMachineName + "/"
				+ KV.COORDINATOR_SERVICE_NAME;
		Client2Coordinator client2Coordinator = (Client2Coordinator) Naming
				.lookup(coordinatorURL);
		runApplication(client2Coordinator);
	}

	/**
	 * Run application.
	 * 
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private static void runApplication(Client2Coordinator client2Coordinator)
			throws RemoteException, InstantiationException,
			IllegalAccessException, ClassNotFoundException {

		client2Coordinator.preProcess();
		String command = "start mine.";
		client2Coordinator.putTask(command);
	}

}
