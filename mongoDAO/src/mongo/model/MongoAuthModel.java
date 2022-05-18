package mongo.model;

/**
 * MongoAuthModel <br>
 * db, user, pwd
 * @author LimDK
 *
 */
public class MongoAuthModel {
	private String USER;
	private String DB;
	private char[] PWD;
	
	/**
	 * Set dbName, user, pwd
	 * @param db
	 * @param user
	 * @param pwd
	 */
	public MongoAuthModel(String db, String user, String pwd) {
		DB = db;
		USER = user;
		PWD = pwd.toCharArray();
	}

	/**
	 * @return DB
	 */
	public String getDB() {
		return DB;
	}
	/**
	 * @return USER
	 */
	public String getUSER() {
		return USER;
	}
	/**
	 * @return PWD
	 */
	public char[] getPWD() {
		return PWD;
	}
}
