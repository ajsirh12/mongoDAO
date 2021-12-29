package mongo.controller;

import java.util.List;

import mongo.dao.MongoDAO;

public class MainController {
	
	private String URL = "172.16.142.130";
	private int PORT = 27017;
	private String DB = "TEST";
	
	public void mainStart() {
		MongoDAO mongoDAO = new MongoDAO(URL, PORT, DB);
		
		mongoDAO.connectMongoDB();
		
		List<Object> list = mongoDAO.selectAll("TEST001");

		for(Object obj : list) {
			System.out.println(obj.toString());
		}
		
		mongoDAO.disconnectMongoDB();
	}
}
