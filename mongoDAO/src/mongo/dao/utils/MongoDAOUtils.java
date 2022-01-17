package mongo.dao.utils;

import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;

import com.mongodb.client.MongoDatabase;

public class MongoDAOUtils {
	private MongoInsert mongoInsert;
	private MongoSelect mongoSelect;
	private MongoUpdate mongoUpdate;
	private MongoDelete mongoDelete;
	
	protected MongoDAOUtils() {
		
	}
	
	protected void setMongoDatabase(MongoDatabase mongoDatabase) {
		mongoInsert = new MongoInsert(mongoDatabase);
		mongoSelect = new MongoSelect(mongoDatabase);
		mongoUpdate = new MongoUpdate(mongoDatabase);
		mongoDelete = new MongoDelete(mongoDatabase);
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
