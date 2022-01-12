package mongo.utils;

import static mongo.utils.MongoConstant.DOCUMENT;
import static mongo.utils.MongoConstant.JSON;
import static mongo.utils.MongoConstant.MAP;
import static mongo.utils.MongoConstant.STRING;
import static mongo.utils.MongoConstant.IDX_ID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import mongo.exceptions.MongoException;

public class MongoUtils {
	
	private static MongoUtils mongoUtils = null;
	
	private static final Filters FILTERS = null;

	private MongoUtils() {
		
	}
	
	/**
	 * Singleton
	 * @author LimDK
	 * @return MongoUtils
	 */
	public static MongoUtils getInstance() {
		if(mongoUtils == null) {
			mongoUtils = new MongoUtils();
		}		
		return mongoUtils;
	}
	
	/**
	 * get FILTERS
	 * @author LimDK
	 * @return Filters
	 */
	public static Filters getFilters() {
		return FILTERS;
	}
	
	/***
	 * Setting mongoDB options <br><br>
	 * unit: ms <br><br>
	 * connectTimeout(timeout) <br>
	 * socketTimeout(timeout) <br>
	 * maxConnectionLifeTime(timeout) <br>
	 * maxConnectionIdleTime(timeout * 20) <br>
	 * 
	 * @author LimDK
	 * @param timeout
	 * @return MongoClientOptions
	 */
	public MongoClientOptions setMongoOptions(int timeout) {
		return MongoClientOptions.builder().connectTimeout(timeout).socketTimeout(timeout)
				.maxConnectionLifeTime(timeout).maxConnectionIdleTime(timeout * 20).build();
	}
	
	/***
	 * ServerAddress List
	 * @author LimDK
	 * @param urls
	 * @param ports
	 * @return List&ltServerAddress&gt
	 */
	public List<ServerAddress> makeServerAddressList(List<String> urls, List<Integer> ports){
		List<ServerAddress> result = new ArrayList<ServerAddress>();
		
		for(int i=0; i<urls.size(); i++) {
			result.add(new ServerAddress(urls.get(i), ports.get(i)));
		}
		
		return result;
	}
	
	/***
	 * List&ltMap&ltString, Object&gt&gt change List&ltDocument&gt
	 * @author LimDK
	 * @param paramList
	 * @return List&ltDocument&gt
	 */
	public List<Document> makeDocList(List<Map<String, Object>> paramList){
		List<Document> docList = new ArrayList<Document>();
		
		for(Map<String, Object> map : paramList) {
			docList.add(new Document(map));
		}
		
		return docList;
	}
	
	/**
	 * 
	 * @author LimDK
	 * @param collection
	 * @param option
	 * @return
	 */
	public List<Object> getSelect(MongoCollection<Document> collection, String option) {
		List<Object> result = new ArrayList<Object>();
		
		FindIterable<Document> iterator = collection.find();
		
		try {
			result = getSelectOption(iterator, option);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * @author LimDK
	 * @param collection
	 * @param option
	 * @param skip
	 * @param limit
	 * @return
	 */
	public List<Object> getSelect(MongoCollection<Document> collection, String option, int skip, int limit) {
		List<Object> result = new ArrayList<Object>();
		
		FindIterable<Document> iterator = collection.find().sort(FILTERS.eq("_id", 1)).skip(skip).limit(limit);
		
		try {
			result = getSelectOption(iterator, option);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	/**
	 * @author LimDK
	 * @param collection
	 * @param option
	 * @param filters
	 * @return
	 */
	public List<Object> getSelect(MongoCollection<Document> collection, String option, Bson filters) {
		List<Object> result = new ArrayList<Object>();
		
		FindIterable<Document> iterator = collection.find(filters);
		
		try {
			result = getSelectOption(iterator, option);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	 * @return
	 */
	public List<Object> getSelect(MongoCollection<Document> collection, String option, Bson filters, int skip, int limit) {
		List<Object> result = new ArrayList<Object>();
		
		FindIterable<Document> iterator = collection.find(filters).sort(FILTERS.eq(IDX_ID, 1)).skip(skip).limit(limit);;
		
		try {
			result = getSelectOption(iterator, option);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
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
	 */
	private List<Object> getSelectOption(FindIterable<Document> iterator, String option) throws JsonParseException, JsonMappingException, IOException{
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
			MongoException.chkOption(option);
		}
		
		return result;
	}
}
