package mongo.dao;

import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import mongo.exceptions.MongoException;
import mongo.dao.utils.MongoDAOUtils;
import mongo.utils.MongoUtils;

public class MongoDAO extends MongoDAOUtils {

	private static final MongoUtils mongoUtils = MongoUtils.getInstance();
	
	private MongoClient MONGO_CLIENT;
	private MongoDatabase MONGO_DATABASE;
	
	private boolean REPL_SET = false;
	
	private String URL;
	private int PORT;
	private String DB;
	
	private List<String> URL_LIST;
	private List<Integer> PORT_LIST;
	
	private int TIMEOUT = 3000;
	
	/***
	 * Do not using MongoDB ReplicaSet
	 * timeout defalut value = 3000ms
	 * @author LimDK
	 * @param url
	 * @param port
	 * @param database
	 */
	public MongoDAO(String url, int port, String database) {
		
		URL = url;
		PORT = port;
		DB = database;
		
		connectMongoDB();
		setMongoDatabase(MONGO_DATABASE);
		
		REPL_SET = false;
	}
	
	/***
	 * Do not using MongoDB ReplicaSet
	 * timeout defalut value = 3000
	 * @author LimDK
	 * @param url
	 * @param port
	 * @param database
	 * @param timeout
	 */
	public MongoDAO(String url, int port, String database, int timeout) {
		URL = url;
		PORT = port;
		DB = database;
		TIMEOUT = timeout;
		
		connectMongoDB();
		setMongoDatabase(MONGO_DATABASE);
		
		REPL_SET = false;
	}
	
	/***
	 * Using MongoDB ReplicaSet
	 * timeout defalut value = 3000
	 * @author LimDK
	 * @param urls
	 * @param ports
	 * @param database
	 */
	public MongoDAO(List<String> urls, List<Integer> ports, String database) {
		// Check Size (urls, ports)
		MongoException.chkSizeException(urls, ports);
		
		URL_LIST = urls;
		PORT_LIST = ports;
		DB = database;
		
		connectMongoDB();
		setMongoDatabase(MONGO_DATABASE);
		
		REPL_SET = true;
	}
	
	/***
	 * Using MongoDB ReplicaSet
	 * timeout defalut value = 3000
	 * @author LimDK
	 * @param urls
	 * @param ports
	 * @param database
	 * @param timeout
	 */
	public MongoDAO(List<String> urls, List<Integer> ports, String database, int timeout) {
		// Check Size (urls, ports)
		MongoException.chkSizeException(urls, ports);
		
		URL_LIST = urls;
		PORT_LIST = ports;
		DB = database;
		TIMEOUT = timeout;
		
		REPL_SET = true;
	}
	
	/***
	 * MongoClient connection
	 * @author LimDK
	 * @return
	 */
	private MongoClient connectClient() {
		MongoClient client = null;
		
		MongoClientOptions options = mongoUtils.setMongoOptions(TIMEOUT);
		
		if(REPL_SET) {
			client = new MongoClient(mongoUtils.makeServerAddressList(URL_LIST, PORT_LIST), options);
		}
		else {
			client = new MongoClient(new ServerAddress(URL, PORT), options);
		}
		
		return client;
	}
	
	/***
	 * MongoDatabase connection
	 * @author LimDK
	 * @param client
	 * @return
	 */
	private MongoDatabase connectDB(MongoClient client) {
		return client.getDatabase(DB);
	}
	
	/***
	 * mongoDB connection
	 * @author LimDK
	 */
	public void connectMongoDB() {
		MONGO_CLIENT = connectClient();
		MONGO_DATABASE = connectDB(MONGO_CLIENT);
	}
	
	/***
	 * mongoDB disconnection
	 * @author LimDK
	 */
	public void disconnectMongoDB() {
		MONGO_CLIENT.close();
	}
	
	/**
	 * get MongoDataBase
	 * @return MongoDatabase
	 */
	public MongoDatabase getDatabase() {
		return MONGO_DATABASE;
	}
}
