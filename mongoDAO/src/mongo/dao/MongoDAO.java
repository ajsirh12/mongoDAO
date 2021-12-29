package mongo.dao;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
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
	 * @param client
	 * @return
	 */
	private MongoDatabase connectDB(MongoClient client) {
		return client.getDatabase(DB);
	}
	
	/***
	 * mongoDB connection
	 */
	public void connectMongoDB() {
		MONGO_CLIENT = connectClient();
		MONGO_DATABASE = connectDB(MONGO_CLIENT);
	}
	
	/***
	 * mongoDB disconnection
	 */
	public void disconnectMongoDB() {
		MONGO_CLIENT.close();
	}
	
	/***
	 * insertOne <br>
	 * @param collectionName
	 * @param param
	 */
	public void insertOne(String collectionName, Map<String, Object> param) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		collection.insertOne(new Document(param));
	}
	
	/***
	 * insertMany <br>
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
	 * @param collectionName
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		FindIterable<Document> iterator = collection.find();
		for(Document doc : iterator) {
			resultMap.add(doc);
		}
		
		return resultMap;
	}
}
