import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;

public class Dstores {
	private static File folderDir;
	private static String fileFolder;
	private static final Logger logger = Logger.getLogger(Controller.class.getName());


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
		} catch (NumberFormatException e) {
			System.err.println("Invalid input format. Please provide integer values for arguments.");
			return;
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("Invalid number of arguments. Please provide 4 integer values for arguments.");
			return;
		}
		fileFolder = folder;
		folderDir = new File(folder);
		if (!folderDir.exists()) {
			System.err.println("The folder path provided does not exist.");
			return;
		}


		try (ServerSocket serverSocket = new ServerSocket(port);
		     Socket controllerSocket = new Socket(InetAddress.getLoopbackAddress(), cport);
		     PrintWriter controllerPrintWriter = new PrintWriter(controllerSocket.getOutputStream(), true);) {


			logger.info("Data Store setup on port " + port);
			controllerPrintWriter.println(String.format("%s %d", Protocol.JOIN_TOKEN, port));
			System.out.println("Data Store setup on port " + port);


			while (true) {
				Socket client = serverSocket.accept();

				new Thread(() -> {
					handleClient(client, controllerPrintWriter, timeout);
				}).start();
			}
		} catch (Exception e) {
			logger.warning("Error: " + e.getMessage());
		}
	}

	private static void handleClient(Socket client, PrintWriter controllerOut, int timeout) {
		while (true) {
			try {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
				PrintWriter out = new PrintWriter(client.getOutputStream(), true);
				String line;
				while ((line = bufferedReader.readLine()) != null) {
					String[] command = line.split(" ");
					switch (command[0]) {
						case Protocol.STORE_TOKEN -> {
							String filename = command[1];
							int fileSize = Integer.parseInt(command[2]);
							if (filename != null) {
								logger.info("Received fileName:" + filename);
								out.println(Protocol.ACK_TOKEN);
							}
							boolean success = receiveFileContent(filename, fileSize, client);
							logger.info("Received file content:" + success);
							if (success) controllerOut.println(Protocol.STORE_ACK_TOKEN +" " + filename);
							logger.info("sent ACK to the controller");
						}
						case Protocol.LOAD_DATA_TOKEN -> {
							String filename = command[1];
							handleLoad(filename, client);

						}
						case Protocol.REMOVE_TOKEN -> {
							String filename = command[1];
							handleRemove(filename,controllerOut);
						}
					}

				}
			} catch (IOException e) {
				logger.warning("IO exception in handleClient: " + e.getMessage());
				throw new RuntimeException(e);
			}
		}

	}

	private static boolean receiveFileContent(String fileName, int fileSize, Socket client) {
		File outputFile = new File(fileFolder, fileName);
		try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
		     InputStream inputStream = client.getInputStream()) {
			logger.info("Starting file reception: " + inputStream);
			byte[] buffer = new byte[1024];
			int bytesRead;
			int remainingBytes = fileSize;
			while (remainingBytes > 0 && (bytesRead = inputStream.read(buffer, 0, Math.min(buffer.length, remainingBytes))) != -1) {
				fileOutputStream.write(buffer, 0, bytesRead);
				remainingBytes -= bytesRead;
			}
			if (remainingBytes == 0) {
				logger.info("File reception complete for " + fileName);
				return true;
			}
			return false;
		} catch (IOException e) {
			return false;
		}
	}

	private static synchronized void handleLoad(String filename, Socket client) {
		sendFileContent(filename, client);
	}

	private static synchronized void sendFileContent(String filename, Socket client) {
		File fileSend = new File(fileFolder, filename);
		try (FileInputStream fileInputStream = new FileInputStream(fileSend)) {
			OutputStream outputStream = client.getOutputStream();
			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = fileInputStream.read(buffer)) != -1) {
				logger.info("Writting contents: " + buffer);
				outputStream.write(buffer, 0, bytesRead);
			}
			outputStream.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private static void handleRemove(String filename,PrintWriter controllerOut) {
		Path path = Paths.get(fileFolder,filename);
		if(path.toFile().exists()) {
			try {
				Files.delete(path);
				controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
				logger.info("Removed file: " + filename);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}else {
			controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
			logger.warning("File does not exist: " + filename);
		}
	}
}

