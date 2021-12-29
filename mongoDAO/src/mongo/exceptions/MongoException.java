package mongo.exceptions;

import java.util.List;

public class MongoException {

	private MongoException() {
		
	}
	
	/***
	 * Do not matching size (urls, ports) <br><br>
	 * throw new IllegalArgumentException("List size exception.  URL_LIST: " + urls.size() + " PORT_LIST: " + ports.size()) <br>
	 * @param urls
	 * @param ports
	 */
	public static void chkSizeException(List<String> urls, List<Integer> ports) {
		if(urls.size() != ports.size()) {
			throw new IllegalArgumentException("List size exception.  URL_LIST: " + urls.size() + " PORT_LIST: " + ports.size());
		}
	}
}
