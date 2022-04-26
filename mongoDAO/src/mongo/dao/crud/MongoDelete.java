package mongo.dao.crud;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;

public class MongoDelete {

	private MongoDatabase MONGO_DATABASE;

	private static MongoDelete mongoDelete = null;

	private MongoDelete(MongoDatabase mongoDatebase) {
		MONGO_DATABASE = mongoDatebase;
	}
	
	public static MongoDelete getInstance(MongoDatabase mongoDatebase) {
		if(mongoDelete == null) {
			mongoDelete = new MongoDelete(mongoDatebase);
		}
		return mongoDelete;
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
