package mongo.dao.utils;

import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import mongo.utils.MongoUtils;

public class MongoInsert {
	
	private MongoDatabase MONGO_DATABASE;
	
	private static final MongoUtils mongoUtils = MongoUtils.getInstance();
	
	public MongoInsert(MongoDatabase mongoDatebase) {
		MONGO_DATABASE = mongoDatebase;
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

}
