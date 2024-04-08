import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

public class IndexToStatecontroller {

	public final static String IS_STORING = "Is_Storing";
	public final static String STORE_COMPLETE = "Store_complete";
	public final static String IS_REMOVING = "Is_Removing";
	public final static String REMOVED = "Removed";

	public static Map<String ,String > dstoreStatusMap;
	private static final Logger logger = Logger.getLogger(IndexToStatecontroller.class.getName());

	public IndexToStatecontroller(Map<String ,String > dstoreStatusMap){
		this.dstoreStatusMap = dstoreStatusMap;
	}

	public String getDstoreStatus (String filename){
		if(dstoreStatusMap.containsKey(filename)){
			return dstoreStatusMap.get(filename);
		}
		logger.warning("The filename does not exist");
		return null;
	}
}
