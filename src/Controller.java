import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.Buffer;
import java.util.logging.Logger;
import java.util.logging.SocketHandler;

public class Controller {

	//Replication number
	private int replicationNumber;
	//
	private BufferedReader bufferedReader;
	//

	private static final Logger logger = Logger.getLogger(Controller.class.getName());


	/**
	 * constructor
	 */
	public Controller(){
		initialize();
		logger.info("Initialize");
	}

	public void initialize(){

	}

	private void handleController (Socket client, int timeOut){
		try {
			PrintWriter out = new PrintWriter( client.getOutputStream(), true);
			bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
			var line = bufferedReader.readLine();

			while( line != null){
				var command = line.split("");
				switch (command[0]){
					case
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}


	}

	private void handleStore(int port){}

	private void handleLoad (int port){}

	private void handleList (int port){}

	private void handleRemove(int port){}
}
