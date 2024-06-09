import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Dstore {
	private static File folderDir;
	private static String fileFolder;
	private static final Logger logger = Logger.getLogger(Dstore.class.getName());
	private static Socket controllerSocket;

	public static void main(String[] args) {
		final int port;
		final int cport;
		final int timeout;
		final String folder;

		try {
			port = Integer.parseInt(args[0]);
			cport = Integer.parseInt(args[1]);
			timeout = Integer.parseInt(args[2]);
			folder = args[3];
		} catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
			System.err.println("Invalid input. Please provide 4 arguments: <port> <cport> <timeout> <folder>");
			return;
		}

		fileFolder = folder;
		folderDir = new File(folder);
		if (!folderDir.exists()) {
			System.err.println("The folder path provided does not exist.");
			return;
		}

		ExecutorService executor = Executors.newCachedThreadPool();

		try (ServerSocket serverSocket = new ServerSocket(port)) {
			controllerSocket = new Socket(InetAddress.getLoopbackAddress(), cport);
			PrintWriter controllerPrintWriter = new PrintWriter(controllerSocket.getOutputStream(), true);

			logger.info("Data Store setup on port " + port);
			controllerPrintWriter.println(String.format("%s %d", Protocol.JOIN_TOKEN, port));

			executor.submit(() -> handleController(controllerSocket, controllerPrintWriter, timeout));

			while (true) {
				Socket client = serverSocket.accept();
				executor.submit(() -> handleClient(client, controllerPrintWriter, timeout));
			}
		} catch (IOException e) {
			logger.warning("Server error: " + e.getMessage());
		}
	}

	private static void handleClient(Socket client, PrintWriter controllerOut, int timeout) {
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
		     PrintWriter out = new PrintWriter(client.getOutputStream(), true)) {

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				logger.info("Received: " + line);
				String[] command = line.split(" ");
				switch (command[0]) {
					case Protocol.STORE_TOKEN -> {

						String filename = command[1];
						int fileSize = Integer.parseInt(command[2]);
						client.setSoTimeout(timeout);  // Set timeout for reading data
						receiveFileContent(filename, fileSize, client, controllerOut,out);
					}
					case Protocol.LOAD_DATA_TOKEN -> {
						String filename = command[1];
						client.setSoTimeout(timeout);  // Set timeout for reading data
						handleLoad(filename, client.getOutputStream());
					}
					case Protocol.REMOVE_TOKEN -> {
						String filename = command[1];
						handleRemove(filename, controllerOut);
					}
					default -> logger.warning("Unknown command: " + command[0]);
				}
			}
		} catch (SocketException e) {
			logger.warning("Socket closed: " + e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.warning("IO exception in handleClient: " + e.getMessage());
		}
	}

	private static void handleController(Socket client, PrintWriter controllerOut, int timeout) {
		try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()))) {
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				logger.info("Received: " + line);
				String[] command = line.split(" ");
				if (Protocol.REMOVE_TOKEN.equals(command[0])) {
					String filename = command[1];
					handleRemove(filename, controllerOut);
				} else {
					logger.warning("Unknown command: " + command[0]);
				}
			}
		} catch (IOException e) {
			logger.warning("IO exception in handleController: " + e.getMessage());
		}
	}


	private static synchronized void receiveFileContent(String fileName, int fileSize, Socket client, PrintWriter controllerOut,PrintWriter out) {
		out.println(Protocol.ACK_TOKEN);
		InputStream inputStream = null;
		try {
			byte[] data = new byte[fileSize];
			inputStream = client.getInputStream();
			inputStream.readNBytes(data, 0, fileSize);
			Path filePath = Paths.get(fileFolder, fileName);
			Files.createDirectories(filePath.getParent());
			Files.write(filePath, data);

			controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
			logger.info("Send ACK to Controller: " + fileName);

		} catch (IOException e) {
			logger.warning("Error receiving file content: " + e.getMessage());
		} finally {
//			try {
//				inputStream.close();
//			} catch (Exception e) {
//				logger.warning("Error receiving file content Dstore: " + e.getMessage());
//			}
		}
	}

	private static synchronized void handleLoad(String fileName, OutputStream out) {
		FileOutputStream fileOutputStream = null;
		try {
			Path path = Paths.get(fileFolder, fileName);
//			fileOutputStream = new FileOutputStream(path.toFile());
			byte[] data = Files.readAllBytes(path);
			out.write(data);
//			fileOutputStream.write(data);
//			fileOutputStream.flush();
		} catch (IOException e) {
			logger.warning("Error loading file: " + e.getMessage());
		} finally {
			if (fileOutputStream != null) {
				try {
					fileOutputStream.close();
				} catch (Exception e) {
					logger.warning("Error loading file22222: " + e.getMessage());
				}
			}
		}
	}

	private static synchronized void handleRemove(String filename, PrintWriter controllerOut) {
		Path path = Paths.get(fileFolder, filename);
		if (Files.exists(path)) {
			try {
				Files.delete(path);
				controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
			} catch (IOException e) {
				logger.warning("Error removing file: " + e.getMessage());
			}
		} else {
			controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
			logger.warning("File does not exist: " + filename);
		}
	}
}
