package mongo.dao.utils;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

public class MongoDelete {

	private MongoDatabase MONGO_DATABASE;
	
	public MongoDelete(MongoDatabase mongoDatebase) {
		MONGO_DATABASE = mongoDatebase;
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
}
