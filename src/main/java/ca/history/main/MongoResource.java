package ca.history.main;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoResource {

	private static volatile MongoClient client;
	private static volatile MongoDatabase db;
	private static volatile MongoCollection<Document> collection;

	public static synchronized MongoClient getClient() {
		if (client == null)
			client = new MongoClient("localhost", 27017);

		return client;
	}

	public static synchronized MongoDatabase getDataBase(String database) {
		if (db == null) {
			if (client == null)
				getClient();
			db = client.getDatabase(database);
		}
		return db;
	}

	public static synchronized MongoCollection<Document> getCollection(String collectionName, String database) {
		if (collection == null) {
			if (db == null)
				getDataBase(database);
			collection = db.getCollection(collectionName);
		}
		return collection;
	}

}