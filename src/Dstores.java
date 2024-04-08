import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class Dstores {
	private static File folderDir;
	private static  String fileFolder;
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
		     Socket controllerSocket = new Socket(InetAddress.getLoopbackAddress(),cport);
		     PrintWriter controllerPrintWriter = new PrintWriter(controllerSocket.getOutputStream(), true);) {


			logger.info("Data Store setup on port " + port);
			controllerPrintWriter.println(String.format("%s %d", Protocol.JOIN_TOKEN, port));
			System.out.println("Data Store setup on port " + port);
			controllerPrintWriter.println("JOIN " + port);

			while (true) {
				Socket client = serverSocket.accept();

				new Thread(() -> {
					handleController(client, controllerPrintWriter, timeout);
				}).start();
			}
		} catch (Exception e) {
			logger.warning("Error: " + e.getMessage());
		}
	}

	private static void handleController(Socket client, PrintWriter controllerOut, int timeout){
		while(true){
			try {
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
				PrintWriter out = new PrintWriter(client.getOutputStream(),true);
				String line;
				while((line = bufferedReader.readLine()) != null){
					String[] command = line.split(" ");
					switch (command[0]){
						case Protocol.STORE_TOKEN -> {
							String fileName = command[1];
							int fileSize = Integer.parseInt(command[2]);
							if(fileName != null){
								logger.info("Received fileName:"+ fileName);
								out.println(Protocol.ACK_TOKEN);
							}
							boolean success = receiveFileContent(fileName,fileSize, client);
							if(success) controllerOut.println(Protocol.STORE_ACK_TOKEN + fileName);
						}
					}

				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}


	}

	private static boolean receiveFileContent(String fileName, int fileSize, Socket client) {
		try {
			FileOutputStream fileOutputStream = new FileOutputStream(new File(fileFolder, fileName));
			try (InputStream inputStream = client.getInputStream()) {
				byte[] buffer = new byte[1024];
				int bytesRead;
				int remainingBytes = fileSize;
				while (remainingBytes > 0 && (bytesRead = inputStream.read(buffer, 0, Math.min(buffer.length, remainingBytes))) != -1) {
					fileOutputStream.write(buffer, 0, bytesRead);
					remainingBytes -= bytesRead;
				}

			} finally {
				fileOutputStream.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

}
