package mongo.dao;

import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import mongo.dao.crud.MongoDelete;
import mongo.dao.crud.MongoInsert;
import mongo.dao.crud.MongoSelect;
import mongo.dao.crud.MongoUpdate;
import mongo.utils.MongoUtils;

public class MongoDAO implements AutoCloseable {

	protected static final MongoUtils mongoUtils = MongoUtils.getInstance();
	
	protected MongoInsert mongoInsert = null;
	protected MongoSelect mongoSelect = null;
	protected MongoUpdate mongoUpdate = null;
	protected MongoDelete mongoDelete = null;
	
	protected MongoClient MONGO_CLIENT = null;
	protected MongoDatabase MONGO_DATABASE = null;
	
	protected boolean REPL_SET = false;
	
	protected String URL;
	protected int PORT;
	protected String DB;
	
	protected List<String> URL_LIST;
	protected List<Integer> PORT_LIST;
	
	protected int TIMEOUT = 3000;
	
	/***
	 * Do not using MongoDB ReplicaSet
	 * timeout default value = 3000ms
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
		
		connectMongoDB();
	}
	
	/***
	 * Do not using MongoDB ReplicaSet
	 * timeout default value = 3000
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
		
		connectMongoDB();
	}
	
	/***
	 * Using MongoDB ReplicaSet
	 * timeout default value = 3000
	 * @author LimDK
	 * @param urls
	 * @param ports
	 * @param database
	 * @throws MongoConstructorException 
	 */
	public MongoDAO(List<String> urls, List<Integer> ports, String database) {
		URL_LIST = urls;
		PORT_LIST = ports;
		DB = database;
		
		REPL_SET = true;
		
		connectMongoDB();
	}
	
	/***
	 * Using MongoDB ReplicaSet
	 * timeout default value = 3000
	 * @author LimDK
	 * @param urls
	 * @param ports
	 * @param database
	 * @param timeout
	 */
	public MongoDAO(List<String> urls, List<Integer> ports, String database, int timeout) {			
		URL_LIST = urls;
		PORT_LIST = ports;
		DB = database;
		TIMEOUT = timeout;
		
		REPL_SET = true;
		
		connectMongoDB();
	}
	
	/***
	 * MongoClient connection
	 * @author LimDK
	 * @return
	 */
	protected MongoClient connectClient() {
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
	protected MongoDatabase connectDB(MongoClient client) {
		return client.getDatabase(DB);
	}
	
	/***
	 * mongoDB connection
	 * @author LimDK
	 */
	public void connectMongoDB() {
		if(MONGO_CLIENT == null) {
			MONGO_CLIENT = connectClient();
		}
		if(MONGO_DATABASE == null) {
			MONGO_DATABASE = connectDB(MONGO_CLIENT);	
		}
		
		// 20220117 LimDK add setCRUD
		setMongoDatabase();
	}
	
	/***
	 * mongoDB disconnection
	 * @author LimDK
	 */
	public void disconnectMongoDB() {
		resetMongoDatabase();
		MONGO_DATABASE = null;
		MONGO_CLIENT.close();
		MONGO_CLIENT = null;
	}
	
	/**
	 * Create MongoCRUD 
	 */
	private void setMongoDatabase() {
		if(mongoInsert == null) {
			mongoInsert = new MongoInsert(MONGO_DATABASE);
		}
		if(mongoSelect == null) {
			mongoSelect = new MongoSelect(MONGO_DATABASE);			
		}
		if(mongoUpdate == null) {
			mongoUpdate = new MongoUpdate(MONGO_DATABASE);	
		}
		if(mongoDelete == null) {
			mongoDelete = new MongoDelete(MONGO_DATABASE);			
		}
	}
	
	/**
	 * set null MongoCRUD
	 */
	private void resetMongoDatabase() {
		mongoInsert = null;
		mongoSelect = null;
		mongoUpdate = null;
		mongoDelete = null;
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
	 * replaceOne
	 * @author LimDK
	 * @param collectionName
	 * @param param
	 * @param filters
	 * @return replaceCount
	 */
	public long replaceOne(String collectionName, Map<String, Object> param, Bson filters) {
		return mongoUpdate.replaceOne(collectionName, param, filters);
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

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		disconnectMongoDB();
	}

}
