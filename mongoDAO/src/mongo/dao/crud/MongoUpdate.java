package mongo.dao.crud;

import static mongo.utils.MongoConstant.SET;

import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;

public class MongoUpdate {
	
	private MongoDatabase MONGO_DATABASE;
	
	public MongoUpdate(MongoDatabase mongoDatebase) {
		MONGO_DATABASE = mongoDatebase;
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
}
