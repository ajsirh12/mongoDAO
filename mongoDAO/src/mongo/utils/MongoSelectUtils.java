package mongo.utils;

import static mongo.utils.MongoConstant.DOCUMENT;
import static mongo.utils.MongoConstant.IDX_ID;
import static mongo.utils.MongoConstant.JSON;
import static mongo.utils.MongoConstant.MAP;
import static mongo.utils.MongoConstant.STRING;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import mongo.exceptions.MongoIllegalOptionException;

public class MongoSelectUtils {

	private static MongoSelectUtils mongoSelect = null;
	private static final Filters FILTERS = null;
	
	private static final Bson sortById = FILTERS.eq(IDX_ID, 1);
	
	private MongoSelectUtils() {
		
	}
	
	public static MongoSelectUtils getInstance() {
		if(mongoSelect == null) {
			mongoSelect = new MongoSelectUtils();
		}
		return mongoSelect;
	}
	
	/**
	 * @author LimDK
	 * @param collection
	 * @param option
	 * @param skip
	 * @param limit
	 * @param sort
	 * @return
	 */
	public List<Object> getSelect(MongoCollection<Document> collection, String option, int skip, int limit, Bson sort){
		List<Object> result = new ArrayList<Object>();
		
		FindIterable<Document> iterator = setSortOption(collection, option, skip, limit, sort);
		
		try {
			result = getSelectOption(iterator, option);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * 
	 * @author LimDK
	 * @param collection
	 * @param option
	 * @param filters
	 * @param skip
	 * @param limit
	 * @param sort
	 * @return
	 */
	public List<Object> getSelect(MongoCollection<Document> collection, String option, Bson filters, int skip, int limit, Bson sort) {
		List<Object> result = new ArrayList<Object>();
		
		FindIterable<Document> iterator = setSortOption(collection, option, filters, skip, limit, sort);
		
		try {
			result = getSelectOption(iterator, option);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return result;
	}

	private FindIterable<Document> setSortOption(MongoCollection<Document> collection, String option, int skip, int limit, Bson sort){
		if(sort == null) {
			return collection.find().sort(sortById).skip(skip).limit(limit);
		}
		else {
			return collection.find().sort(sort).skip(skip).limit(limit);
		}
	}
	
	private FindIterable<Document> setSortOption(MongoCollection<Document> collection, String option, Bson filters, int skip, int limit, Bson sort){
		if(sort == null) {
			return collection.find(filters).sort(sortById).skip(skip).limit(limit);
		}
		else {
			return collection.find(filters).sort(sort).skip(skip).limit(limit);
		}
	}
	
	/**
	 * 
	 * @author LimDK
	 * @param iterator
	 * @param option
	 * @return
	 * @throws JsonParseException
	 * @throws JsonMappingException
	 * @throws IOException
	 * @throws MongoIllegalOptionException 
	 */
	private List<Object> getSelectOption(FindIterable<Document> iterator, String option) throws JsonParseException, JsonMappingException, IOException, MongoIllegalOptionException{
		List<Object> result = new ArrayList<Object>();
		
		if(option == null) {
			for(Document doc : iterator) {
				result.add(doc);
			}
		}
		else if(option.equals(DOCUMENT)) {
			for(Document doc : iterator) {
				result.add(doc);
			}
		}
		else if(option.equals(MAP)) {
			ObjectMapper mapper = new ObjectMapper();
			for(Document doc : iterator) {
				Map<String, Object> param = mapper.readValue(doc.toJson(), Map.class);
				result.add(param);
			}
		}
		else if(option.equals(JSON)) {
			for(Document doc : iterator) {
				result.add(doc.toJson());
			}
		}
		else if(option.equals(STRING)) {
			for(Document doc : iterator) {
				result.add(doc.toString());
			}
		}
		else {
			throw new MongoIllegalOptionException(option);
		}
		
		return result;
	}
}
