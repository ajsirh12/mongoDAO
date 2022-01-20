package mongo.exceptions;

public class MongoIllegalOptionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7997942424365955665L;
	private String msg;
	private static final String defaultMsg = "Unsupported Option parameter. (\"Document\", \"Map\", \"Json\", \"String\")";
	
	
	/**
	 * Unsupported Option parameter.<br>
	 * Unsupported Option parameter. ("Document", "Map", "Json", "String")<br>
	 */
	public MongoIllegalOptionException() {
		super();
	}
	
	/**
	 * Unsupported Option parameter.<br>
	 * "Option" is Unsupported option parameter. ("Document", "Map", "Json", "String")<br>
	 * @param msg
	 */
	public MongoIllegalOptionException(String option) {
		super(option);
		this.msg = fillMessage(option);
	}
	
	@Override
	public String getMessage() {
		return msg==null?defaultMsg:msg;
	}
	
	private String fillMessage(String option) {
		return "\"" + option + "\" is Unsupported option parameter. (\"Document\", \"Map\", \"Json\", \"String\")";
	}
}
