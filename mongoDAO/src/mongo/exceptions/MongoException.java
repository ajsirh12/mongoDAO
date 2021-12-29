package mongo.exceptions;

import java.util.List;

public class MongoException {

	private MongoException() {
		
	}
	
	/***
	 * Do not matched size (urls, ports) <br><br>
	 * throw new IllegalArgumentException("List size exception.  URL_LIST: " + urls.size() + " PORT_LIST: " + ports.size()) <br>
	 * @param urls
	 * @param ports
	 */
	public static void chkSizeException(List<String> urls, List<Integer> ports) {
		if(urls.size() != ports.size()) {
			throw new IllegalArgumentException("List size exception.  URL_LIST: " + urls.size() + " PORT_LIST: " + ports.size());
		}
	}
	
	/***
	 * Do not included in option parameter
	 * throw new IllegalArgumentException("\"" + option + "\" is not option parameter.")<br>
	 * @param option
	 */
	public static void chkOption(String option) {
		throw new IllegalArgumentException("\"" + option + "\" is not option parameter.");
	}
}
