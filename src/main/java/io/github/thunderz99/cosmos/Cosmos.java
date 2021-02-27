package io.github.thunderz99.cosmos;

import java.util.List;

/**
 * class that represent a cosmos account
 *
 * Usage:
 * var cosmos = new Cosmos("AccountEndpoint=https://xxx.documents.azure.com:443/;AccountKey=xxx==;");
 * var db = cosmos.getDatabase("Database1");
 *
 * //Then use db to do CRUD / query
 * db.upsert("Users", user);
 *
 */

import java.util.regex.Pattern;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.thunderz99.cosmos.util.Checker;

/***
 * class that represent a cosmos account
 *
 * <pre>
 * Usage: var cosmos = new
 * Cosmos("AccountEndpoint=https://xxx.documents.azure.com:443/;AccountKey=xxx==;");
 * var db = cosmos.getDatabase("Database1");
 *
 * //Then use db to do CRUD / query db.upsert("Users", user);
 * </pre>
 *
 */
public class Cosmos implements AutoCloseable {

	private static Logger log = LoggerFactory.getLogger(Cosmos.class);

	CosmosClient client;

	String account = "";

	static Pattern p = Pattern.compile("AccountEndpoint=(?<endpoint>.+);AccountKey=(?<key>.+);");

	static Pattern accountPattern = Pattern.compile("https://(?<account>[\\w\\-]+)\\..*");

	public Cosmos(String connectionString) {

		Pair<String, String> pair = parseConnectionString(connectionString);
		var endpoint = pair.getLeft();
		var key = pair.getRight();

		this.account = parseAcount(endpoint);

		this.client = new CosmosClientBuilder()
				.endpoint(endpoint)
				.key(key)
				.preferredRegions(List.of("Japan East", "West US"))
				.consistencyLevel(ConsistencyLevel.SESSION)
				.contentResponseOnWriteEnabled(true)
				.buildClient();
	}

	static Pair<String, String> parseConnectionString(String connectionString) {

		var matcher = p.matcher(connectionString);
		if (!matcher.find()) {
			throw new IllegalStateException(
					"Make sure connectionString contains 'AccountEndpoint=' and 'AccountKey=' ");
		}
		String endpoint = matcher.group("endpoint");
		String key = matcher.group("key");

		Checker.check(StringUtils.isNotBlank(endpoint), "Make sure connectionString contains 'AccountEndpoint=' ");
		Checker.check(StringUtils.isNotBlank(key), "Make sure connectionString contains 'AccountKey='");

		if(log.isInfoEnabled()){
			log.info("endpoint:{}", endpoint);
			log.info("key:{}...", key.substring(0, 3));
		}

		return Pair.of(endpoint, key);
	}


	static String parseAcount(String endpoint) {
		var matcher = accountPattern.matcher(endpoint);
		if (!matcher.find()) {
			throw new IllegalStateException(
					"Make sure endpoint matches https://xxx.yyy ");
		}
		var account = matcher.group("account");
		Checker.check(StringUtils.isNotBlank(account), "Make sure account is correct");

		return account;
	}


	/**
     * Get a CosmosDatabase object by name
	 *
	 * @param db database name
	 * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        Checker.checkNotEmpty(db, "db");
		return new CosmosDatabase(client, db, account);
    }


	/**
	 * Create the db and coll if not exist. Coll creation will be skipped is empty.
	 * @param db database name
	 * @param coll collection name
	 * @return CosmosDatabase instance
	 */
	public CosmosDatabase createIfNotExist(String db, String coll) {
		client.createDatabaseIfNotExists(db);
		createCollectionIfNotExist(db, coll);
        return new CosmosDatabase(client, db, account);
    }



	/**
	 * Delete a collection by db name and coll name
	 * @param db database name
	 * @param coll collection name
	 */
	public void deleteCollection(String db, String coll) {
		var collectionLink = getCollectionLink(db, coll);
		try {
			Checker.checkNotNull(client, "client");
			Checker.checkNotBlank(db, "db");
			Checker.checkNotBlank(coll, "coll");
			var database = client.getDatabase(db);
			var container = database.getContainer(coll);
			container.delete();
			log.info("delete Collection:{}, account:{}", collectionLink, this.account);
		} catch (RuntimeException e) {
			// If not exist
			if (isResourceNotFoundException(e)) {
				log.info("delete Collection not exist. Ignored:{}, account:{}", collectionLink, this.account);
			} else {
				// Throw any other Exception
				throw e;
			}
		}
	}

	/**
	 * Delete a database by name
	 * @param db database name
	 */
	public void deleteDatabase(String db) {
		try {
			Checker.checkNotNull(client, "client");
			Checker.checkNotBlank(db, "db");
			var database = client.getDatabase(db);
			database.delete();
			log.info("delete Database:{}, account:{}", db, account);
		} catch (RuntimeException e) {
			// If not exist
			if (isResourceNotFoundException(e)) {
				log.info("delete Database not exist. Ignored:{}, account:{}", db, account);
			} else {
				// Throw any other Exception
				throw e;
			}
		}
	}


	public CosmosContainer createCollectionIfNotExist(String db, String coll) {

		var database = client.getDatabase(db);

		var containerProperties =
				new CosmosContainerProperties(coll, "/" + getDefaultPartitionKey());
		containerProperties.setDefaultTimeToLiveInSeconds(-1);

		//  Create container with 400 RU/s
		var throughputProperties = ThroughputProperties.createManualThroughput(400);
		var containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties);
		var container = database.getContainer(containerResponse.getProperties().getId());

		var databaseLink = getDatabaseLink(db);
		log.info("create Collection:{}/colls/{}, account:{}", databaseLink, coll, account);

		return container;

	}


	static String getDatabaseLink(String db) {
		return String.format("/dbs/%s", db);
	}

	static String getCollectionLink(String db, String coll) {
		return String.format("/dbs/%s/colls/%s", db, coll);
	}

	static String getDocumentLink(String db, String coll, String id) {
		return String.format("/dbs/%s/colls/%s/docs/%s", db, coll, id);
	}

	static boolean isResourceNotFoundException(Exception e) {
		var message = e.getMessage() == null ? "" : e.getMessage();
		return message.contains("Not Found") ? true : false;
	}

	/**
	 * get the default partition key
	 * @return default partition key
	 */
	public static String getDefaultPartitionKey() {
		return "_partition";
	}

	String getAccount(){
		return this.account;
	}

	@Override
	public void close(){
		if(this.client != null){
			this.client.close();
			this.client = null;
		}
	}
}