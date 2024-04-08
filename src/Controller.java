import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class Controller {

	//Replication number
	private int replicationNumber;
	//
	private BufferedReader bufferedReader;
	private static IndexToStatecontroller index;
	// Map index into socket
	private static Map<Integer, Socket> dataStoreSockets;
	// Map filename into fileStore status
	private static Map<String, PrintWriter> fileStorageMap;
	//Map filename into fileStore status
	private static Map<String, PrintWriter> fileRemoveMap;
	private static Map<String, Integer> FilenameSizeMap;
	private static Map<String, Integer> receiveAck;


	// Map fileName into list of Dstore
	private static Map<String, List<Integer>> fileToDstoreMap;

	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

	private static final Logger logger = Logger.getLogger(Controller.class.getName());


	/**
	 * constructor
	 */
	public Controller() {
		index = new IndexToStatecontroller(new ConcurrentHashMap<>());
		initialize();

	}

	public void initialize() {
		dataStoreSockets = new ConcurrentHashMap<>();
		fileStorageMap = new ConcurrentHashMap<>();
		fileRemoveMap = new ConcurrentHashMap<>();
		fileToDstoreMap = new ConcurrentHashMap<>();
		FilenameSizeMap = new ConcurrentHashMap<>();
		receiveAck = new ConcurrentHashMap<>();
		logger.info("Initialize");

	}

	private void handleController(Socket client, int timeOut) {
		int dataStorePort = 0;
		boolean DstoreConnection = false;
		try {
			PrintWriter out = new PrintWriter(client.getOutputStream(), true);
			bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
			var line = bufferedReader.readLine();

			while (line != null) {
				var command = line.split(" ");
				switch (command[0]) {
					case Protocol.JOIN_TOKEN -> {
						DstoreConnection = true;
						logger.info("DataStore Join " + DstoreConnection);
						dataStorePort = Integer.parseInt(command[1]);
						addDstore(dataStorePort, client);
						handleBalance();
					}
					case Protocol.STORE_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleStore(dataStorePort, command, replicationNumber, out);
						}
					}
					case Protocol.STORE_ACK_TOKEN -> {
						handleDstoreAcks(command[1], out, timeOut);
					}
					case Protocol.LOAD_TOKEN -> {

					}
					case Protocol.REMOVE_TOKEN -> {

					}
					case Protocol.LIST_TOKEN -> {
						if (DstoreConnection && dataStorePort != 0) {
							handleList(dataStorePort, command);
						} else {

						}
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}


	private synchronized void handleStore(int port, String[] command, int replicationNumber, PrintWriter out) {
		logger.info(" It is handling storage, port : " + port);
		if (!IndexToStatecontroller.dstoreStatusMap.containsKey(command[1])) {
			IndexToStatecontroller.dstoreStatusMap.put(command[1], IndexToStatecontroller.IS_STORING);
			FilenameSizeMap.put(command[1], Integer.parseInt(command[2]));
			sendStoreCommandToClient(out, replicationNumber);
		} else {
			out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
		}

	}

	private synchronized void handleLoad(int port) {
	}

	private synchronized void handleRemove() {
	}

	private synchronized void handleList(int port, String[] command) {
		rebalance++;
		List<String> fileNames = new ArrayList<>(Arrays.asList(command).subList(1, command.length));
		updateFileToDStore(port, fileNames);

	}

	private synchronized void updateFileToDStore(int port, List<String> filenames) {
		if (!fileToDstoreMap.isEmpty()) {
			for (String eachFile : filenames
			) {
				if (fileToDstoreMap.containsKey(eachFile)) {
					fileToDstoreMap.get(eachFile).add(port);
				}
			}
		}

	}

	private synchronized void handleRemove(int port) {
	}

	private synchronized void handleBalance() {
	}

	private synchronized void addDstore(int port, Socket client) {
		dataStoreSockets.put(port, client);
		logger.info("Added dataStore, port, client:" + port + "," + client);
	}

	private synchronized boolean checkDstoreEnough(Socket client, PrintWriter out) {
		if (dataStoreSockets.size() >= replicationNumber) {
			return true;
		} else {
			out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
			logger.info(client + "Not enough data store spaces");
		}
		return false;
	}

	private List<Integer> selectDstores(int replicationNumber) {
		List<Integer> availablePorts = new ArrayList<>(dataStoreSockets.keySet());
		List<Integer> ports = availablePorts.subList(0, replicationNumber);
		logger.info("Select the number of ports :" + ports.size());
		return ports;
	}

	private void sendStoreCommandToClient(PrintWriter out, int replicationNumber) {
		List<Integer> ports = selectDstores(replicationNumber);
		StringBuilder stringBuilder = new StringBuilder();
		for (Integer port : ports) {
			stringBuilder.append(" ").append(port);
		}
		String dstores = stringBuilder.toString();
		out.println(Protocol.STORE_TO_TOKEN + dstores);
	}

	private synchronized void handleDstoreAcks(String filename, PrintWriter out, int timeout) {
		boolean allAcksReceived = false;
		if (receiveAck.containsKey(filename)) {
			int countAck = receiveAck.get(filename);
			receiveAck.put(filename, countAck + 1);
		} else {
			receiveAck.put(filename, 1);
		}

		checkEnoughAck(filename, out);

		Runnable timeoutTask = () -> {
			if (receiveAck.getOrDefault(filename, 0) < replicationNumber) {
				IndexToStatecontroller.dstoreStatusMap.remove(filename);
			}
		};

		scheduler.schedule(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	private synchronized void checkEnoughAck(String filename, PrintWriter out) {

		if (receiveAck.get(filename) >= replicationNumber) {
			IndexToStatecontroller.dstoreStatusMap.put(filename, IndexToStatecontroller.STORE_COMPLETE);
			out.println(Protocol.STORE_COMPLETE_TOKEN);
			logger.info("Enough ACKs received");
		}
	}
}
