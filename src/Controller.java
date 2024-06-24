import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.*;
import java.util.logging.Formatter;

public class Controller {
	// Replication number
	private static int replicationNumber;
	private static IndexToStatecontroller index;
	private static ConcurrentMap<Integer, Socket> dataStoreSockets;
	private static ConcurrentMap<String, PrintWriter> fileClient;
	private static ConcurrentMap<String, PrintWriter> fileRemoveClient;
	private static ConcurrentMap<String, Integer> filenameSizeMap;
	private static ConcurrentMap<Socket, Set<Integer>> lastUsedPortByClient;
    private static ConcurrentMap<String, ConcurrentMap<PrintWriter, Integer>> latchMap;
	private static ConcurrentMap<String, ConcurrentMap<PrintWriter, Integer>> latchRemoveMap;
	private static ConcurrentMap<String, List<Integer>> fileToDstoreMap;
	private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
	private static final Logger logger = Logger.getLogger(Controller.class.getName());
	private static ConcurrentMap<Socket, Integer> loadCount;
    private final ReadWriteLock removeLock = new ReentrantReadWriteLock();
    private final ReadWriteLock storeLock = new ReentrantReadWriteLock();
    private final ReadWriteLock listLock = new ReentrantReadWriteLock();
	ArrayList a = new ArrayList();



    private static void configureLogger() {
        logger.setLevel(Level.ALL);
        logger.setUseParentHandlers(false);
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.ALL);
        Formatter formatter = new SimpleFormatter() {
            @Override
            public synchronized String format(LogRecord lr) {
                String format = "[%1$tF %1$tT.%1$tL] [%2$-7s] %3$s %n";
                return String.format(format, new Date(lr.getMillis()), lr.getLevel().getLocalizedName(),
                        lr.getMessage());
            }
        };
        consoleHandler.setFormatter(formatter);
        logger.addHandler(consoleHandler);
    }

	public Controller() {
		initialize();
	}

	public void initialize() {
		dataStoreSockets = new ConcurrentHashMap<>();
		fileToDstoreMap = new ConcurrentHashMap<>();
		fileClient = new ConcurrentHashMap<>();
		filenameSizeMap = new ConcurrentHashMap<>();
		lastUsedPortByClient = new ConcurrentHashMap<>();
		latchMap = new ConcurrentHashMap<>();
		latchRemoveMap = new ConcurrentHashMap<>();
		fileRemoveClient = new ConcurrentHashMap<>();
		index = new IndexToStatecontroller();
		loadCount = new ConcurrentHashMap<>();
        configureLogger();
		logger.info("Initialize");
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			logger.warning("Insufficient arguments provided. Expected 4 arguments.");
			return;
		}

		final int controllerPort = Integer.parseInt(args[0]);
		final int replicationNumber = Integer.parseInt(args[1]);
		int timeout = Integer.parseInt(args[2]);
		int rebalancePeriod = Integer.parseInt(args[3]);

		Controller controller = new Controller();
		controller.setReplicationFactor(replicationNumber);

		ExecutorService pool = Executors.newCachedThreadPool();

		try (ServerSocket serverSocket = new ServerSocket(controllerPort)) {
			logger.info("Controller started on port " + controllerPort);

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
		String line;
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
		     PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {

			while ((line = bufferedReader.readLine()) != null) {
				var command = line.split(" ");
				logger.info("Received command: " + command[0]);
				switch (command[0]) {
					case Protocol.JOIN_TOKEN -> {
						dataStorePort = Integer.parseInt(command[1]);
						logger.info("DataStore Join ");
						addDstore(dataStorePort, client);
						client.setKeepAlive(true);
					}
					case Protocol.STORE_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleStore(client, command[1], command[2], replicationNumber, out, timeOut);
						}
					}
					case Protocol.STORE_ACK_TOKEN -> {
						logger.info("STORE_ACK back for:  " + command[1]);
						handleDstoreAcks(command[1], out);
					}
					case Protocol.LOAD_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleLoad(command[1], out, client);
						}
					}
					case Protocol.RELOAD_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleReload(command[1], out, client);
						}
					}
					case Protocol.REMOVE_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleRemove(command[1], out, timeOut);
						}
					}
					case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
						handleRemoveAck(command[1], out);
					}
					case Protocol.LIST_TOKEN -> {
						if (checkDstoreEnough(client, out)) {
							handleList(dataStorePort, command, out);
						}
					}
					default -> logger.warning("Unknown command: " + command[0]);
				}
			}
		} catch (SocketException e) {
			logger.warning("Dstore " + dataStorePort + " was killed");
			e.printStackTrace();
		} catch (IOException e) {
			logger.warning("Error in communication: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.warning("Unexpected error: " + e.getMessage());
			e.printStackTrace();
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

    private void handleStore(Socket client, String filename, String fileSize, int replicationNumber, PrintWriter out, int timeout) {
        boolean canStore = false;

        storeLock.writeLock().lock();
        try {
            String previousState = index.getDstoreStatus(filename);
            if (previousState != null) {
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                logger.warning("File is already being stored or removed, filename: " + filename);
                return;
            }
            index.setDstoreStatus(filename, IndexToStatecontroller.IS_STORING);
            filenameSizeMap.put(filename, Integer.parseInt(fileSize));
            logger.info("It is handling storage  : " + filename + " " + out.toString());
            canStore = true;
        } finally {
            storeLock.writeLock().unlock();
        }

        if (canStore) {
            sendStoreCommandToClient(client, out, replicationNumber, filename, timeout);
        }
    }

	private void handleLoad(String filename, PrintWriter out, Socket client) {
		List<Integer> ports = selectDstores(replicationNumber);
		loadCount.put(client, ports.size() - 1);

		if (!index.existsDstoreStatus(filename) || index.getDstoreStatus(filename) == IndexToStatecontroller.IS_STORING || index.getDstoreStatus(filename) == IndexToStatecontroller.IS_REMOVING) {
			out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
			logger.warning("File does not exist in the index, filename: " + filename);
			return;
		}
		List<Integer> availablePorts = fileToDstoreMap.get(filename);
		int count = loadCount.get(client);
		Integer candidatePort = availablePorts.get(count);

		if (candidatePort == null) {
			logger.warning("Not enough Dstores available or no valid port found.");
			return;
		}

		int fileSize = filenameSizeMap.get(filename);
		out.println(Protocol.LOAD_FROM_TOKEN + " " + candidatePort + " " + fileSize);
		logger.info("reload: " + candidatePort + " " + fileSize);
		addPortIfAbsent(client, candidatePort);
	}

	private void handleReload(String filename, PrintWriter out, Socket client) {
		if (!index.existsDstoreStatus(filename) || index.getDstoreStatus(filename) == IndexToStatecontroller.IS_STORING || index.getDstoreStatus(filename) == IndexToStatecontroller.IS_REMOVING) {
			out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
			logger.warning("File does not exist in the index(Storing or Removing), filename: " + filename);
			return;
		}

		int count = loadCount.get(client);
		logger.info("before : " + count);
		loadCount.replace(client, count - 1);
		int newCount = loadCount.get(client);
		logger.info("after : " + newCount);

		if (newCount < 0) {
			out.println(Protocol.ERROR_LOAD_TOKEN);
			logger.warning("Not enough ports in storedFileDstore or no available ports.");
		} else {
			List<Integer> availablePorts = fileToDstoreMap.get(filename);
			Integer candidatePort = availablePorts.get(newCount);
			int fileSize = filenameSizeMap.get(filename);
			out.println(Protocol.LOAD_FROM_TOKEN + " " + candidatePort + " " + fileSize);
			logger.info("reload: " + candidatePort + " " + fileSize);
			addPortIfAbsent(client, candidatePort);
		}
	}

    private void handleRemove(String filename, PrintWriter out, int timeout) {
        AtomicBoolean canRemove = new AtomicBoolean(false);
        removeLock.writeLock().lock();
        try {
            String previousState = index.getDstoreStatus(filename);
            if (previousState == null || previousState == IndexToStatecontroller.IS_STORING || previousState == IndexToStatecontroller.IS_REMOVING) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                logger.warning("File does not exist in the index(Storing or Removing), filename: " + filename);
                return;
            }
            index.updateSetStatus(filename, IndexToStatecontroller.IS_REMOVING);
            canRemove.set(true);
        } finally {
            removeLock.writeLock().unlock();
        }

        if (canRemove.get()) {
            sendRemoveToDstore(out, filename, timeout);
        }
    }

	private void sendRemoveToDstore(PrintWriter out, String filename, Integer timeout) {
		List<Integer> ports;
        removeLock.readLock().lock();
        try {
            if (!fileToDstoreMap.containsKey(filename)) {
                logger.info("fileToDstoreMap do not contain: " + filename);
                return;
            }
            ports = new ArrayList<>(fileToDstoreMap.get(filename));
            fileRemoveClient.put(filename, out);

            ConcurrentMap<PrintWriter, Integer> latch = new ConcurrentHashMap<>();
            latch.put(out, replicationNumber);
            latchRemoveMap.put(filename,latch);
            logger.info(latchRemoveMap.toString());
            logger.info(String.valueOf(latchRemoveMap.get(filename).get(out)));
            int i = latchRemoveMap.get(filename).get(out);
            logger.info("Remove count created = " + i);

        } finally {
            removeLock.readLock().unlock();
        }


        for (Integer port : ports) {
            sendRemove(port, filename);
        }


        scheduler.schedule(() -> {

            if (latchRemoveMap.containsKey(filename) && latchRemoveMap.get(filename).containsKey(out) && latchRemoveMap.get(filename).equals(out) && latchRemoveMap.get(filename).get(out) > 0) {
                logger.warning("Timeout expired for removing file: " + filename);
                handleRemoveTimeout(filename, out);
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void handleRemoveTimeout(String filename, PrintWriter out) {

        removeLock.writeLock().lock();
        try {
            latchRemoveMap.get(filename).remove(out);
            if (latchRemoveMap.containsKey(filename) && latchRemoveMap.get(filename) == null){
                latchRemoveMap.remove(filename);
            }
            logger.warning("Timeout occurred before all remove ACKs were received for " + filename);
        } finally {
            removeLock.writeLock().unlock();
        }
    }

    private void handleList(int port, String[] command, PrintWriter out) {
        listLock.readLock().lock();
        try {
            List<String> fileNames = new ArrayList<>(filenameSizeMap.keySet());
            if (fileNames.isEmpty()) {
                out.println(Protocol.LIST_TOKEN);
                logger.info("No files to list");
            } else {
                StringBuilder stringBuilder = new StringBuilder();
                for (String filename : fileNames) {
                    if (index.existsDstoreStatus(filename) && Objects.equals(index.getDstoreStatus(filename), IndexToStatecontroller.STORE_COMPLETE)) {
                        logger.info("state: " + index.getDstoreStatus(filename));
                        stringBuilder.append(" ").append(filename);
                    }
                }
                String filenames = stringBuilder.toString();
                out.println(Protocol.LIST_TOKEN + filenames);
                logger.info("Sent protocol to client " + Protocol.LIST_TOKEN + filenames);
            }
        } finally {
            listLock.readLock().unlock();
        }
    }


    private void addDstore(int port, Socket client) {
		if (!dataStoreSockets.containsKey(port)) {
			dataStoreSockets.put(port, client);
			logger.info("Added dataStore, port, client:" + port + "," + client);
		} else {
			logger.warning("Attempted to add an existing DataStore on port: " + port);
		}
	}

	private boolean checkDstoreEnough(Socket client, PrintWriter out) {
		if (dataStoreSockets.size() >= replicationNumber) {
			return true;
		} else {
			out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
			logger.info(client + " Not enough data store spaces");
		}
		return false;
	}

	private List<Integer> selectDstores(int replicationNumber) {
		List<Integer> availablePorts = new ArrayList<>(dataStoreSockets.keySet());
		return availablePorts.subList(0, replicationNumber);
	}

    private void sendStoreCommandToClient(Socket client, PrintWriter out, int replicationNumber, String filename, int timeout) {
        List<Integer> storedFileDstore = new ArrayList<>();
        List<Integer> ports = selectDstores(replicationNumber);

        StringBuilder stringBuilder = new StringBuilder();
        for (Integer port : ports) {
            storedFileDstore.add(port);
            stringBuilder.append(" ").append(port);
        }
        String dstores = stringBuilder.toString();
        out.println(Protocol.STORE_TO_TOKEN + dstores);
        fileToDstoreMap.put(filename, storedFileDstore);
        fileClient.put(filename, out);
        logger.info("fileClient: " + filename + " " + out);
        ConcurrentMap<PrintWriter, Integer> latch = new ConcurrentHashMap<>();
        latch.put(out, replicationNumber);
        latchMap.put(filename,latch);
        logger.info(latchMap.toString());
        logger.info(String.valueOf(latchMap.get(filename).get(out)));
        int i = latchMap.get(filename).get(out);
        logger.info("Store count created = " + i);

        scheduler.schedule(() -> {
            if (latchMap.containsKey(filename) && latchMap.get(filename).containsKey(out) && latchMap.get(filename).equals(out) && latchMap.get(filename).get(out) > 0) {
                processTimeout(filename, out);
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }


    private synchronized void handleDstoreAcks(String filename, PrintWriter out) {
        var c = fileClient.get(filename);
        logger.info(c.toString());
		if (latchMap.containsKey(filename) && fileClient.containsKey(filename) && latchMap.get(filename) != null) {
            logger.info("count = " + latchMap.get(filename).get(c));
			int i = latchMap.get(filename).get(c);
            int newCount = i -1;
            ConcurrentMap<PrintWriter, Integer> latch = new ConcurrentHashMap<>();
            latch.put(c, newCount);
			latchMap.put(filename, latch);
		}

        int count = latchMap.get(filename).get(c);

		logger.info("count dstore: " + filename + " " + count + "=========================");
		if (count == 0) {
			processComplete(filename, out);
		} else {
			logger.warning("File does not have corresponding store count");
		}
	}

	private void processComplete(String filename, PrintWriter out) {
		fileClient.get(filename).println(Protocol.STORE_COMPLETE_TOKEN);
		index.updateSetStatus(filename, IndexToStatecontroller.STORE_COMPLETE);
		logger.info("STORE COMPLETE ****** Enough Store ACKs received for " + filename);
	}

    private void processTimeout(String filename, PrintWriter out) {
        storeLock.writeLock().lock();
        try {
            index.removeDstoreStatus(filename);
            filenameSizeMap.remove(filename);
            latchMap.get(filename).remove(out);
            if (latchMap.containsKey(filename) && latchMap.get(filename) == null){
                latchMap.remove(filename);
            }
            logger.warning("Timeout occurred before all store ACKs were received for " + filename);
        } finally {
            storeLock.writeLock().unlock();
        }
    }


	private void sendRemove(Integer port, String fileName) {
		Socket dataStore = dataStoreSockets.get(port);
		if (dataStore != null) {
			try {
				PrintWriter dataStorePrintWriter = new PrintWriter(dataStore.getOutputStream(), true);
				dataStorePrintWriter.println(Protocol.REMOVE_TOKEN + " " + fileName);
				logger.info("remove send to " + dataStore.getPort());
				dataStorePrintWriter.flush();
			} catch (IOException e) {
				logger.warning("Failed to send remove request to port " + port + ": " + e.getMessage());
			}
		} else {
			logger.warning("No DataStore connected at port: " + port);
		}
	}

	private synchronized void handleRemoveAck(String filename, PrintWriter out) {
        var c = fileRemoveClient.get(filename);

		if (latchRemoveMap.containsKey(filename) && fileRemoveClient.containsKey(filename) && latchRemoveMap.get(filename) != null) {
            logger.info("count = " + latchRemoveMap.get(filename).get(c));
            int i = latchRemoveMap.get(filename).get(c);
            int newCount = i -1;
            ConcurrentMap<PrintWriter, Integer> latch = new ConcurrentHashMap<>();
            latch.put(c, newCount);
            latchRemoveMap.put(filename, latch);
		}
        int newCount = latchRemoveMap.get(filename).get(c);

        logger.info("cLatch remove: " + filename + " " + newCount + "==========================================");

        if (newCount == 0) {
			processReComplete(filename, out);
		}
	}

	private void processReComplete(String filename, PrintWriter out) {
		fileRemoveClient.get(filename).println(Protocol.REMOVE_COMPLETE_TOKEN);
		index.removeDstoreStatus(filename);
		filenameSizeMap.remove(filename);
		fileToDstoreMap.remove(filename);
        latchRemoveMap.remove(filename);
		logger.info("All REMOVE_ACKs received for file: " + filename);
		logger.info("STORE COMPLETE ****** Enough Remove ACKs received for " + filename);
	}

	public void addPortIfAbsent(Socket client, int port) {
		lastUsedPortByClient.computeIfAbsent(client, k -> ConcurrentHashMap.newKeySet()).add(port);
	}
}
