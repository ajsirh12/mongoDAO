package mongo.utils;

public enum MongoConstant {
	DOCUMENT("DOCUMENT"),
	MAP("MAP"),
	JSON("JSON"),
	STRING("STRING"),
	IDX_ID("_id"),
	SET("$set");
	
	private final String value;
	
	MongoConstant(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return this.value;
	}
}