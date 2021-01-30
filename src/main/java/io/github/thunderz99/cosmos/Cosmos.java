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

import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DataType;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.Index;
import com.microsoft.azure.documentdb.IndexingMode;
import com.microsoft.azure.documentdb.IndexingPolicy;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;

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
public class Cosmos {

	private static Logger log = LoggerFactory.getLogger(Cosmos.class);

	DocumentClient client;

	String account;

	static Pattern p = Pattern.compile("AccountEndpoint=(?<endpoint>.+);AccountKey=(?<key>.+)");

	public Cosmos(String connectionString) {

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

		this.client = new DocumentClient(endpoint, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
	}


	/**
     * Get a CosmosDatabase object by name
	 *
	 * @param db database name
	 * @return CosmosDatabase instance
     */
    public CosmosDatabase getDatabase(String db) {
        Checker.checkNotEmpty(db, "db");
		return new CosmosDatabase(client, db);
    }


	/**
	 * Create the db and coll if not exist. Coll creation will be skipped is empty.
	 * @param db database name
	 * @param coll collection name
	 * @return CosmosDatabase instance
	 * @throws DocumentClientException Cosmos client exception Cosmos client exception
	 */
	public CosmosDatabase createIfNotExist(String db, String coll) throws DocumentClientException {

        if (StringUtils.isBlank(coll)) {
            createDatabaseIfNotExist(client, db);
        } else {
			createCollectionIfNotExist(client, db, coll);
        }

        return new CosmosDatabase(client, db);
    }

	/**
	 * Delete a database by name
	 * @param db database name
	 * @throws DocumentClientException Cosmos client exception
	 */
	public void deleteDatabase(String db) throws DocumentClientException {
		deleteDatabase(client, db);
	}

	/**
	 * Delete a collection by db name and coll name
	 * @param db database name
	 * @param coll collection name
	 * @throws DocumentClientException Cosmos client exception
	 */
	public void deleteCollection(String db, String coll) throws DocumentClientException {
		deleteDatabase(client, db);
	}

	static Database createDatabaseIfNotExist(DocumentClient client, String db) throws DocumentClientException {

		var database = readDatabase(client, db);
		if (database != null) {
			return database;
		}

		var account = getAccount(client);

		var dbObj = new Database();
		dbObj.setId(db);
		var options = new RequestOptions();
		options.setOfferThroughput(400);
		var result = client.createDatabase(dbObj, options);
		log.info("created database:{}, account:{}", db, getAccount(client));
		return result.getResource();
	}

	static Database readDatabase(DocumentClient client, String db) throws DocumentClientException {
        try {
            Checker.checkNotBlank(db, "db");
            var res =
                client.readDatabase(getDatabaseLink(db), null);
            return res.getResource();
        } catch (DocumentClientException de) {
            // If not exist
            if (isResourceNotFoundException(de)) {
                return null;
            } else {
                // Throw any other Exception
				throw de;
            }
        }
    }

	static void deleteDatabase(DocumentClient client, String db) throws DocumentClientException {
		try {
			Checker.checkNotNull(client, "client");
			Checker.checkNotBlank(db, "db");
			client.deleteDatabase(getDatabaseLink(db), null);
			log.info("delete Database:{}, account:{}", db, getAccount(client));
		} catch (DocumentClientException de) {
			// If not exist
			if (isResourceNotFoundException(de)) {
				log.info("delete Database not exist. Ignored:{}, account:{}", db, getAccount(client));
			} else {
				// Throw any other Exception
				throw de;
			}
		}
	}

	static void deleteCollection(DocumentClient client, String db, String coll) throws DocumentClientException {
		var collectionLink = getCollectionLink(db, coll);
		try {
			Checker.checkNotNull(client, "client");
			Checker.checkNotBlank(db, "db");
			Checker.checkNotBlank(coll, "coll");
			client.deleteCollection(collectionLink, null);
			log.info("delete Collection:{}, account:{}", collectionLink, getAccount(client));
		} catch (DocumentClientException de) {
			// If not exist
			if (isResourceNotFoundException(de)) {
				log.info("delete Collection not exist. Ignored:{}, account:{}", collectionLink, getAccount(client));
			} else {
				// Throw any other Exception
				throw de;
			}
		}
	}

	static DocumentCollection createCollectionIfNotExist(DocumentClient client, String db, String coll) throws DocumentClientException {

		createDatabaseIfNotExist(client, db);

        var collection = readCollection(client, db, coll);

        if(collection != null) {
        	return collection;
        }

		var collectionInfo = new DocumentCollection();
		collectionInfo.setId(coll);

		collectionInfo.setIndexingPolicy(getDefaultIndexingPolicy());
		collectionInfo.setDefaultTimeToLive(-1);

		var partitionKeyDef = new PartitionKeyDefinition();
		var paths = List.of("/" + getDefaultPartitionKey());
		partitionKeyDef.setPaths(paths);

		collectionInfo.setPartitionKey(partitionKeyDef);

		var databaseLink = getDatabaseLink(db);
		var createdColl = client.createCollection(databaseLink, collectionInfo, null);
		log.info("create Collection:{}/colls/{}, account:{}", databaseLink, coll, getAccount(client));

		return createdColl.getResource();

	}

	static DocumentCollection readCollection(DocumentClient client, String db, String coll)
			throws DocumentClientException {
		try {
			var res = client.readCollection(getCollectionLink(db, coll), null);
			return res.getResource();
		} catch (DocumentClientException de) {
			// if Not Found
			if (de.getStatusCode() == 404) {
				return null;
			} else {
				// Throw other exceptions
				throw de;
			}
		}
	}

	/**
	 * return the default indexingPolicy
	 */
	static IndexingPolicy getDefaultIndexingPolicy() {
		var rangeIndexOverride = new Index[3];
		rangeIndexOverride[0] = Index.Range(DataType.Number, -1);
		rangeIndexOverride[1] = Index.Range(DataType.String, -1);
		rangeIndexOverride[2] = Index.Spatial(DataType.Point);
		var indexingPolicy = new IndexingPolicy(rangeIndexOverride);
		indexingPolicy.setIndexingMode(IndexingMode.Consistent);
		log.info("set indexing policy to default: {} ", indexingPolicy);
		return indexingPolicy;
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
		if (e instanceof DocumentClientException) {
			var de = (DocumentClientException) e;
			return (de.getStatusCode() == 404);
		}
		var message = e.getMessage() == null ? "" : e.getMessage();
		return message.contains("Resource Not Found") ? true : false;
	}

	/**
	 * get the default partition key
	 * @return default partition key
	 */
	public static String getDefaultPartitionKey() {
		return "_partition";
	}

	String getAccount() throws DocumentClientException {
		if(StringUtils.isNotEmpty(this.account)){
			return this.account;
		}
		this.account = getAccount(this.client);
		return this.account;
	}

	/**
	 * Get cosmos db account id from client
	 * @param client documentClient
	 * @return cosmos db account id
	 * @throws DocumentClientException Cosmos client exception
	 */
	static String getAccount(DocumentClient client) throws DocumentClientException {
		if(Objects.isNull(client)){
			return "";
		}
		return client.getDatabaseAccount().get("id").toString();
	}
}