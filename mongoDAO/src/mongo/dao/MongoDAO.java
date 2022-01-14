package mongo.dao;

import static mongo.utils.MongoConstant.SET;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import mongo.exceptions.MongoException;
import mongo.utils.MongoSelectUtils;
import mongo.utils.MongoUtils;

public class MongoDAO {

	private static final MongoUtils mongoUtils = MongoUtils.getInstance();
	// Add 20220114 LimDK
	private static final MongoSelectUtils mongoSelect = MongoSelectUtils.getInstance();
	
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
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, null, 0, 0, null);
		
		return resultMap;
	}
	
	/**
	 * selectAll<br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param skip
	 * @param limit
	 * @return
	 */
	public List<Object> selectAll(String collectionName, int skip, int limit){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, null, skip, limit, null);
		
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

		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, option, 0, 0, null);
		
		return resultMap;
	}
	
	/**
	 * selectAll with option<br>
	 * option List : {<br>
	 * &emsp;"Document", <br>
	 * &emsp;"Map", <br>
	 * &emsp;"Json", <br>
	 * &emsp;"String"<br>
	 * }<br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param option
	 * @param skip
	 * @param limit
	 * @return
	 */
	public List<Object> selectAll(String collectionName, String option, int skip, int limit){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, option, skip, limit, null);
		
		return resultMap;
	}

	/***
	 * selectWhere <br>
	 * @author LimDK
	 * @param collectionName
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, null, filters, 0, 0, null);
		
		return resultMap;
	}
	
	/**
	 * selectWhere <br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @param skip
	 * @param limit
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters, int skip, int limit){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, null, filters, skip, limit, null);
		
		return resultMap;
	}
	
	/***
	 * selectWhere with option<br>
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
	public List<Object> selectWhere(String collectionName, String option, Bson filters){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, option, filters, 0, 0, null);
		
		return resultMap;
	}
	
	/**
	 * selectWhere with option<br>
	 * option List : {<br>
	 * &emsp;"Document", <br>
	 * &emsp;"Map", <br>
	 * &emsp;"Json", <br>
	 * &emsp;"String"<br>
	 * }<br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param option
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters, int skip, int limit){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelect.getSelect(collection, option, filters, skip, limit, null);
		
		return resultMap;
	}
	
	/**
	 * updateOne<br>
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 * @param filters
	 * @return updateCount
	 */
	public long updateOne(String collectionName, Map<String, Object> param, Bson filters) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		UpdateResult result = collection.updateOne(filters, new Document(SET, new Document(param)));
		
		return result.getModifiedCount();
	}
	
	/**
	 * updateMany<br>
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 * @param filters
	 * @return updateCount
	 */
	public long updateMany(String collectionName, Map<String, Object> param, Bson filters) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		UpdateResult result = collection.updateMany(filters, new Document(SET, new Document(param)));
		
		return result.getModifiedCount();
	}
	
	/**
	 * deleteOne<br>
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @return deleteCount
	 */
	public long deleteOne(String collectionName, Bson filters) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		DeleteResult result = collection.deleteOne(filters);
		
		return result.getDeletedCount();
	}
	
	/**
	 * deleteMany<br>
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @return deleteCount
	 */
	public long deleteMany(String collectionName, Bson filters) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		DeleteResult result = collection.deleteMany(filters);
		
		return result.getDeletedCount();
	}
	
	/**
	 * @author LimDK
	 * @param collectionName
	 * @return
	 */
	public long selectAllCount(String collectionName){
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		return collection.countDocuments();
	}
	
	/**
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @return
	 */
	public long selectWhereCount(String collectionName, Bson filters) {
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		return collection.countDocuments(filters);
	}
}
