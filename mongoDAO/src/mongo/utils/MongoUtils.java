package mongo.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

public class MongoUtils {
	
	private static MongoUtils mongoUtils = null;

	private MongoUtils() {
		
	}
	
	public static MongoUtils getInstance() {
		if(mongoUtils == null) {
			mongoUtils = new MongoUtils();
		}		
		return mongoUtils;
	}
	
	/***
	 * Setting mongoDB options <br><br>
	 * unit: ms <br><br>
	 * connectTimeout(timeout) <br>
	 * socketTimeout(timeout) <br>
	 * maxConnectionLifeTime(timeout) <br>
	 * maxConnectionIdleTime(timeout * 20) <br>
	 * 
	 * @param timeout
	 * @return MongoClientOptions
	 */
	public MongoClientOptions setMongoOptions(int timeout) {
		return MongoClientOptions.builder().connectTimeout(timeout).socketTimeout(timeout)
				.maxConnectionLifeTime(timeout).maxConnectionIdleTime(timeout * 20).build();
	}
	
	/***
	 * ServerAddress List
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
}