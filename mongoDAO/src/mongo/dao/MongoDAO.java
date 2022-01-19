package mongo.dao;

import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import mongo.dao.utils.MongoDelete;
import mongo.dao.utils.MongoInsert;
import mongo.dao.utils.MongoSelect;
import mongo.dao.utils.MongoUpdate;
import mongo.exceptions.MongoException;
import mongo.utils.MongoUtils;

public class MongoDAO {

	private static final MongoUtils mongoUtils = MongoUtils.getInstance();
	
	private MongoInsert mongoInsert;
	private MongoSelect mongoSelect;
	private MongoUpdate mongoUpdate;
	private MongoDelete mongoDelete;
	
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

		// 20220117 LimDK add setCRUD
		setMongoDatabase();
	}
	
	/***
	 * mongoDB disconnection
	 * @author LimDK
	 */
	public void disconnectMongoDB() {
		MONGO_CLIENT.close();
	}
	
	private void setMongoDatabase() {
		mongoInsert = new MongoInsert(MONGO_DATABASE);
		mongoSelect = new MongoSelect(MONGO_DATABASE);
		mongoUpdate = new MongoUpdate(MONGO_DATABASE);
		mongoDelete = new MongoDelete(MONGO_DATABASE);
	}
	
	/***
	 * insertOne <br>
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 */
	public void insertOne(String collectionName, Map<String, Object> param) {
		mongoInsert.insertOne(collectionName, param);
	}
	
	/***
	 * insertMany <br>
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 */
	public void insertMany(String collectionName, List<Map<String, Object>> param) {
		mongoInsert.insertMany(collectionName, param);
	}
	
	/***
	 * selectAll <br>
	 * @author LimDK
	 * @param collectionName
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName){
		return mongoSelect.selectAll(collectionName);
	}
	
	/**
	 * selectAll<br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param skip
	 * @param limit
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName, int skip, int limit){
		return mongoSelect.selectAll(collectionName, skip, limit);
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
		return mongoSelect.selectAll(collectionName, option);
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
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option, int skip, int limit){
		return mongoSelect.selectAll(collectionName, option, skip, limit);
	}

	/***
	 * selectWhere <br>
	 * @author LimDK
	 * @param collectionName
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters){
		return mongoSelect.selectWhere(collectionName, filters);
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
		return mongoSelect.selectWhere(collectionName, filters, skip, limit);
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
	 * @param filters
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters){
		return mongoSelect.selectWhere(collectionName, option, filters);
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
	 * @param filters
	 * @param skip
	 * @param limit
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters, int skip, int limit){
		return mongoSelect.selectWhere(collectionName, option, filters, skip, limit);
	}
	
	/***
	 * selectAll <br>
	 * @author LimDK
	 * @param collectionName
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName, Bson sort){
		return mongoSelect.selectAll(collectionName, sort);
	}
	
	/**
	 * selectAll<br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param skip
	 * @param limit
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName, int skip, int limit, Bson sort){
		return mongoSelect.selectAll(collectionName, skip, limit, sort);
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
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option, Bson sort){
		return mongoSelect.selectAll(collectionName, option, sort);
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
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option, int skip, int limit, Bson sort){
		return mongoSelect.selectAll(collectionName, option, skip, limit, sort);
	}

	/***
	 * selectWhere <br>
	 * @author LimDK
	 * @param collectionName
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters, Bson sort){
		return mongoSelect.selectWhere(collectionName, filters, sort);
	}
	
	/**
	 * selectWhere <br>
	 * search data from "skip" to "skip + limit" 
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @param skip
	 * @param limit
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters, int skip, int limit, Bson sort){
		return mongoSelect.selectWhere(collectionName, filters, skip, limit, sort);
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
	 * @param filters
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters, Bson sort){
		return selectWhere(collectionName, option, filters, sort);
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
	 * @param filters
	 * @param skip
	 * @param limit
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters, int skip, int limit, Bson sort){
		return mongoSelect.selectWhere(collectionName, option, filters, skip, limit, sort);
	}
	
	/**
	 * @author LimDK
	 * @param collectionName
	 * @return
	 */
	public long selectAllCount(String collectionName){
		return mongoSelect.selectAllCount(collectionName);
	}
	
	/**
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @return
	 */
	public long selectWhereCount(String collectionName, Bson filters) {
		return mongoSelect.selectWhereCount(collectionName, filters);
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
		return mongoUpdate.updateOne(collectionName, param, filters);
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
		return mongoUpdate.updateMany(collectionName, param, filters);
	}

	/**
	 * deleteOne<br>
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @return deleteCount
	 */
	public long deleteOne(String collectionName, Bson filters) {
		return mongoDelete.deleteOne(collectionName, filters);
	}
	
	/**
	 * deleteMany<br>
	 * @author LimDK
	 * @param collectionName
	 * @param filters
	 * @return deleteCount
	 */
	public long deleteMany(String collectionName, Bson filters) {
		return mongoDelete.deleteMany(collectionName, filters);
	}

}
