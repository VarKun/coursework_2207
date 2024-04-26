import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class Controller {

	//Replication number
	private  int replicationNumber;
	//
	private BufferedReader bufferedReader;
	private static IndexToStatecontroller index;
	// Map index into socket
	private static Map<Integer, Socket> dataStoreSockets;
	private static List<Integer> storedFileDstore;
	// Map filename into fileStore status
	private static Map<String, PrintWriter> fileStorageMap;
	//Map filename into fileStore status
	private static Map<String, Integer> fileRemoveAck;
	private static Map<String, Integer> filenameSizeMap;
	private static Map<String, Integer> receiveAck;
	private static Map<Socket, Integer> lastUsedPortByClient;
	// Declare this map as a class member
	private Map<String, Future<?>> removeAcksFutures;



	// Map fileName into list of Dstore
	private static Map<String, List<Integer>> fileToDstoreMap;

	private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

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
		fileRemoveAck = new ConcurrentHashMap<>();
		fileToDstoreMap = new ConcurrentHashMap<>();
		filenameSizeMap = new ConcurrentHashMap<>();
		receiveAck = new ConcurrentHashMap<>();
		storedFileDstore = new ArrayList<>();
		lastUsedPortByClient = new ConcurrentHashMap<>();
		removeAcksFutures = new ConcurrentHashMap<>();
		logger.info("Initialize");

	}
	public static void main(String[] args) {
		if (args.length < 4) {
			logger.warning("Insufficient arguments provided. Expected 4 arguments.");
			return;
		}

		int controllerPort = Integer.parseInt(args[0]);
		int replicationNumber = Integer.parseInt(args[1]);
		int timeout = Integer.parseInt(args[2]);
		int rebalancePeriod = Integer.parseInt(args[3]);


		Controller controller = new Controller();
		controller.setReplicationFactor(replicationNumber);
		controller.startRebalanceTimer(rebalancePeriod * 1000);

		try (var serverSocket = new ServerSocket(controllerPort);){
			logger.info("Controller started on port " + controllerPort);

			ExecutorService pool = Executors.newFixedThreadPool(10);

			while (true) {
				Socket client = serverSocket.accept();
				pool.submit(() -> controller.handleController(client, timeout));
			}
		} catch (IOException e) {
			logger.warning("Error starting server socket: " + e.getMessage());
		}
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

							handleStore(dataStorePort, command[1], command[2], replicationNumber, out);
						}
					}
					case Protocol.STORE_ACK_TOKEN -> {
						handleDstoreAcks(command[1], out, timeOut);
					}
					case Protocol.LOAD_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleLoad(command[1], out, replicationNumber,client);
						}
					}
					case Protocol.RELOAD_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleReload(command[1], out, replicationNumber,true,client);
						}
					}
					case Protocol.REMOVE_TOKEN -> {
						if(checkDstoreEnough(client, out)) {
							handleRemove(command[1], out, timeOut);
						}

					}
					case Protocol.REMOVE_ACK_TOKEN,Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
						handleRemoveAck(command[1],out );
					}
					case Protocol.LIST_TOKEN -> {
						if (DstoreConnection && dataStorePort != 0 && checkDstoreEnough(client,out)) {
							handleList(dataStorePort, command, out);
						} else {

						}
					}
					default -> logger.warning("Unknown command: " + command[0]);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	private void setReplicationFactor(int replicationFactor) {
		this.replicationNumber = replicationFactor;
	}

	private void startRebalanceTimer(long period) {
		scheduler.scheduleAtFixedRate(this::rebalance, 0, period, TimeUnit.MILLISECONDS);
	}

	private void rebalance() {
		// Implementation of rebalance logic goes here
		logger.info("Rebalancing the system...");
		// Potentially iterate over `fileToDstoreMap` and adjust according to current needs
	}


	private synchronized void handleStore(int port, String filename,String fileSize, int replicationNumber, PrintWriter out) {
		logger.info(" It is handling storage, port : " + port);
		if (!IndexToStatecontroller.dstoreStatusMap.containsKey(filename)) {
			IndexToStatecontroller.dstoreStatusMap.put(filename, IndexToStatecontroller.IS_STORING);
			filenameSizeMap.put(fileSize, Integer.parseInt(fileSize));
			sendStoreCommandToClient(out, replicationNumber,filename);
		} else {
			out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
		}

	}
	private synchronized void handleLoad(String filename, PrintWriter out, int replicationNumber,Socket client) throws IOException {
		if(IndexToStatecontroller.dstoreStatusMap.containsKey(filename)) {
			Random random = new Random();
			int candidatePort = storedFileDstore.get(random.nextInt(storedFileDstore.size()));
			int fileSize = filenameSizeMap.get(filename);
			if(storedFileDstore.size() >= replicationNumber ) {
				 int port = candidatePort;
				out.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize);
				lastUsedPortByClient.put(client, port);

			}else {
				logger.warning("Not enough ports in storedFileDstore" + storedFileDstore.size() );
			}

		}else {
			out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
			logger.warning("  File does not exist in the index, filename : " + filename);
			closeClientConnection(client);
		}
	}


	private synchronized void handleReload(String filename, PrintWriter out, int replicationNumber,boolean reload,Socket client){
		if(IndexToStatecontroller.dstoreStatusMap.containsKey(filename)) {
			int port = -1;
			Integer lastUsedPort = lastUsedPortByClient.getOrDefault(client,-1);
			Random random = new Random();
			Optional<Integer> newPort = storedFileDstore.stream()
					.filter(p -> !p.equals(lastUsedPort))
					.findAny();
			int fileSize = filenameSizeMap.get(filename);

			if(storedFileDstore.size() >= replicationNumber && newPort.isPresent()) {
				port = newPort.get();
				out.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize);
				lastUsedPortByClient.put(client, port);

			}else {
				logger.warning("Not enough ports in storedFileDstore or Unavailable Port" + storedFileDstore.size() + newPort.isPresent() );

			}

			if(port == -1 ){
				out.println(Protocol.ERROR_LOAD_TOKEN);
				logger.warning(" Client cannot connect to or receive data from any of the R Dstores");
			}

		}else {
			out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
			logger.warning("  File does not exist in the index, filename : " + filename);
			closeClientConnection(client);
		}
	}

	private synchronized void handleRemove(String filename, PrintWriter out, int timeout) {
		if(!IndexToStatecontroller.dstoreStatusMap.containsKey(filename)){
			out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
			logger.info("File does not exist in the index, filename : " + filename);
			return;
		}

		IndexToStatecontroller.dstoreStatusMap.put(filename, IndexToStatecontroller.IS_REMOVING);
		logger.info("Is removing " + filename);


		if(fileToDstoreMap.containsKey(filename)){
			List<Integer> ports = fileToDstoreMap.get(filename);
			fileRemoveAck.put(filename, 0);
			for(Integer port : ports ){
				scheduler.execute( ()->sendRemove(port, filename));
			}
			// Schedule the timeout task
			Runnable timeoutTask = () -> {
				Integer ackCount = fileRemoveAck.getOrDefault(filename, 0);
				if (ackCount < ports.size() ) {
					logger.warning("Timeout occurred for REMOVE operation of file: " + filename);

				}
			};

			Future<?> timeoutFuture = scheduler.schedule(timeoutTask, timeout, TimeUnit.MILLISECONDS);
			fileRemoveAck.put(filename, 0); // Initialize ack counter for this filename
			// Store the future task to be able to cancel it later
			removeAcksFutures.put(filename, timeoutFuture);
		}
	}

	private synchronized void handleList(int port, String[] command, PrintWriter out) {
//		rebalance++;
		List<String> fileNames = new ArrayList<>(filenameSizeMap.keySet());
		if (fileNames.size() == 0) {
			out.println(Protocol.LIST_TOKEN );
		}else {
			StringBuilder stringBuilder = new StringBuilder();
			for(String filename: fileNames){
				if(index.getDstoreStatus(filename) == IndexToStatecontroller.STORE_COMPLETE){
					stringBuilder.append(" ").append(filename);
				}
			}
			String filenames = stringBuilder.toString();
			out.println(Protocol.LIST_TOKEN + filenames );
		}

//		updateFileToDStore(port, fileNames);

	}

//	private synchronized void updateFileToDStore(int port, List<String> filenames) {
//		if (!fileToDstoreMap.isEmpty()) {
//			for (String eachFile : filenames
//			) {
//				if (fileToDstoreMap.containsKey(eachFile)) {
//					fileToDstoreMap.get(eachFile).add(port);
//				}
//			}
//		}
//
//	}

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

	private void sendStoreCommandToClient(PrintWriter out, int replicationNumber, String filename) {
		List<Integer> ports = selectDstores(replicationNumber);
		StringBuilder stringBuilder = new StringBuilder();
		for (Integer port : ports) {
			storedFileDstore.add(port);
			stringBuilder.append(" ").append(port);
		}
		String dstores = stringBuilder.toString();
		out.println(Protocol.STORE_TO_TOKEN + dstores);
		fileToDstoreMap.put(filename,storedFileDstore);
	}

	private synchronized void handleDstoreAcks(String filename, PrintWriter out, int timeout) {
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
					logger.info("Enough Store ACKs received");
				}
	}



	private void closeClientConnection(Socket client) {
		try {
			client.close();
			logger.info("Client closed");
		} catch (IOException e) {
			logger.severe("Failed to close client socket: " + e.getMessage());
		}
	}


	private synchronized void sendRemove(int port, String filename) {
		Socket dataStoreSocket = dataStoreSockets.get(port);
		if (dataStoreSocket != null) {
			try(PrintWriter dstoresPrintWriter = new PrintWriter(dataStoreSocket.getOutputStream(),true)) {
				dstoresPrintWriter.println(Protocol.REMOVE_TOKEN +" " + filename);
				logger.info("Sending remove " + filename);
			} catch (IOException e) {
				logger.warning("Failed to send remove " + filename);
				throw new RuntimeException(e);
			}
		}
	}

	private synchronized void handleRemoveAck(String filename, PrintWriter out) {
		Integer ackCount = fileRemoveAck.compute(filename, (k, v) -> (v == null) ? 1 : v + 1);

		if (ackCount >= fileToDstoreMap.get(filename).size()) {
			// Cancel the timeout task as all acks have been received
			Future<?> timeoutFuture = removeAcksFutures.remove(filename);
			if (timeoutFuture != null) {
				timeoutFuture.cancel(false);
			}
			IndexToStatecontroller.dstoreStatusMap.remove(filename);
			out.println(Protocol.REMOVE_COMPLETE_TOKEN);
			filenameSizeMap.remove(filename);
			logger.info("All REMOVE_ACKs received for file: " + filename);
		}
	}

}
