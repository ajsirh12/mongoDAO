package mongo.dao.crud;

import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

import mongo.utils.MongoConstant;

public class MongoUpdate {
	
	private MongoDatabase MONGO_DATABASE;
	
	private static MongoUpdate mongoUpdate = null;
	
	private MongoUpdate() {
		
	}
	
	public static MongoUpdate getInstance() {
		if(mongoUpdate == null) {
			mongoUpdate = new MongoUpdate();
		}
		return mongoUpdate;
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
		
		UpdateResult result = collection.updateOne(filters, new Document(MongoConstant.SET.getValue(), new Document(param)));
		
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
		
		UpdateResult result = collection.updateMany(filters, new Document(MongoConstant.SET.getValue(), new Document(param)));
		
		return result.getModifiedCount();
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
		MongoCollection<Document> collection = MONGO_DATABASE.getCollection(collectionName);
		
		UpdateResult result = collection.replaceOne(filters, new Document(param));
		
		return result.getModifiedCount();
	}
}
