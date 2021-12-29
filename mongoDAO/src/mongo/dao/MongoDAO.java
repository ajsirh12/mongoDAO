package mongo.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import mongo.exceptions.MongoException;
import mongo.utils.MongoUtils;

public class MongoDAO {

	private static final MongoUtils mongoUtils = MongoUtils.getInstance();
	
	private MongoClient MONGO_CLIENT;
	private MongoDatabase MONGO_DATABASE;
	
	private boolean REPL_SET = false;
	
	private String URL;
	private int PORT;
	private String DB;
	
	private List<String> URL_LIST;
	private List<Integer> PORT_LIST;
	
	private static final int TIMEOUT = 3000;
	
	/***
	 * Do not using MongoDB ReplicaSet
	 * @author LimDK
	 * @param url
	 * @param port
	 * @param database
	 */
	public MongoDAO(String url, int port, String database) {
		URL = url;
		PORT = port;
		DB = database;
		
		REPL_SET = false;
	}
	
	/***
	 * Using MongoDB ReplicaSet
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
	
	/***
	 * insertOne <br>
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 */
	public void insertOne(String collectionName, Map<String, Object> param) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		collection.insertOne(new Document(param));
	}
	
	/***
	 * insertMany <br>
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 */
	public void insertMany(String collectionName, List<Map<String, Object>> param) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		List<Document> docList = mongoUtils.makeDocList(param);
		collection.insertMany(docList);
	}
	
	/***
	 * selectAll <br>
	 * @author LimDK
	 * @param collectionName
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		resultMap = mongoUtils.getSelect(collection, null);
		
		return resultMap;
	}
	
	/***
	 * selectAll with option<br>
	 * option List : {<br>
	 * &emsp;"Document", <br>
	 * &emsp;"Map", <br>
	 * &emsp;"Json", <br>
	 * &emsp;"String"<br>
	 * }
	 * @author LimDK
	 * @param collectionName
	 * @param option
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);

		resultMap = mongoUtils.getSelect(collection, option);
		
		return resultMap;
	}

}
