package mongo.dao.crud;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import mongo.utils.MongoSelectUtils;

public class MongoSelect {
	
	private static final MongoSelectUtils mongoSelectUtils = MongoSelectUtils.getInstance();

	private MongoDatabase MONGO_DATABASE;
	
	private static MongoSelect mongoSelect = null;
	
	private MongoSelect() {
		
	}
	
	public static MongoSelect getInstance() {
		if(mongoSelect == null) {
			mongoSelect = new MongoSelect();
		}
		return mongoSelect;
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
		resultMap = mongoSelectUtils.getSelect(collection, null, 0, 0, null);
		
		return resultMap;
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
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, null, skip, limit, null);
		
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
		resultMap = mongoSelectUtils.getSelect(collection, option, 0, 0, null);
		
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
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option, int skip, int limit){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, skip, limit, null);
		
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
		resultMap = mongoSelectUtils.getSelect(collection, null, filters, 0, 0, null);
		
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
		resultMap = mongoSelectUtils.getSelect(collection, null, filters, skip, limit, null);
		
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
	 * @param filters
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, filters, 0, 0, null);
		
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
	 * @param filters
	 * @param skip
	 * @param limit
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters, int skip, int limit){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, filters, skip, limit, null);
		
		return resultMap;
	}
	
	/***
	 * selectAll <br>
	 * @author LimDK
	 * @param collectionName
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectAll(String collectionName, Bson sort){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, null, 0, 0, sort);
		
		return resultMap;
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
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, null, skip, limit, sort);
		
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
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option, Bson sort){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);

		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, 0, 0, sort);
		
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
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectAll(String collectionName, String option, int skip, int limit, Bson sort){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, skip, limit, sort);
		
		return resultMap;
	}

	/***
	 * selectWhere <br>
	 * @author LimDK
	 * @param collectionName
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters, Bson sort){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, null, filters, 0, 0, sort);
		
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
	 * @param sort : null is default {"_id":1}
	 * @return List&ltDocument&gt
	 */
	public List<Object> selectWhere(String collectionName, Bson filters, int skip, int limit, Bson sort){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, null, filters, skip, limit, sort);
		
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
	 * @param filters
	 * @param sort : null is default {"_id":1}
	 * @return Document : List&ltDocument&gt <br>
	 * Map : List&ltMap&ltString, Object&gt&gt <br>
	 * Json : List&ltJson&gt <br>
	 * String : List&ltString&gt <br>
	 */
	public List<Object> selectWhere(String collectionName, String option, Bson filters, Bson sort){
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, filters, 0, 0, sort);
		
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
		List<Object> resultMap = new ArrayList<Object>();
		
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		// Refactor 20220114 LimDK
		resultMap = mongoSelectUtils.getSelect(collection, option, filters, skip, limit, sort);
		
		return resultMap;
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
