package mongo.model;

public class MongoAuthModel {
	private String USER;
	private String DB;
	private char[] PWD;
	
	public MongoAuthModel(String db, String user, String pwd) {
		DB = db;
		USER = user;
		PWD = pwd.toCharArray();
	}

	public String getDB() {
		return DB;
	}
	public String getUSER() {
		return USER;
	}
	public char[] getPWD() {
		return PWD;
	}
}
