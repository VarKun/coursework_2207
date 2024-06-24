import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class IndexToStatecontroller {

	public final static String IS_STORING = "Is_Storing";
	public final static String STORE_COMPLETE = "Store_complete";
	public final static String IS_REMOVING = "Is_Removing";

	//no use
	public final static String REMOVED = "Removed";

	public static Map<String, String> dstoreStatusMap;
	private static final Logger logger = Logger.getLogger(IndexToStatecontroller.class.getName());

	public IndexToStatecontroller() {
		dstoreStatusMap = new ConcurrentHashMap<>();
	}

	public String getDstoreStatus(String filename) {
		logger.info("Getting status for " + filename);
		if (dstoreStatusMap.containsKey(filename)) {
			return dstoreStatusMap.get(filename);
		} else {
			logger.warning("The filename does not exist");
		}
		return null;
	}

	public synchronized void setDstoreStatus(String filename, String status) {
		if (!dstoreStatusMap.containsKey(filename)) {
			dstoreStatusMap.put(filename, status);
			logger.info("The filename has been set: " + filename + " to " + status);
		}
	}

	public synchronized void updateSetStatus(String filename, String status) {
		if (dstoreStatusMap.containsKey(filename)) {
			dstoreStatusMap.put(filename, status);
			logger.info("The filename has been updated: " + filename + " to " + status);
		}
	}

	public synchronized boolean existsDstoreStatus(String filename) {
		logger.info("existsDstoreStatus: " + dstoreStatusMap.containsKey(filename));
		return dstoreStatusMap.containsKey(filename);
	}

	public synchronized void removeDstoreStatus(String filename) {
		dstoreStatusMap.remove(filename);
		logger.info("removeDstoreStatus: " + filename);
	}

	public Set<String> findName() {
		return dstoreStatusMap.keySet();
	}
}
