package io.github.thunderz99.cosmos;

import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class representing a database instance.
 *
 * <p>
 *     Can do document' CRUD and find.
 * </p>
 */
public class CosmosDatabase {

	private static Logger log = LoggerFactory.getLogger(CosmosDatabase.class);

	String db;

	String account;

	DocumentClient client;

	Cosmos cosmosAccount;

	CosmosDatabase(DocumentClient client, String db, Cosmos cosmosAccount) {
		this.client = client;
		this.db = db;
		this.cosmosAccount = cosmosAccount;
	}


	/**
	 * Create a document
	 * @param coll collection name
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos Client Exception
	 */
	public CosmosDocument create(String coll, Object data, String partition) throws Exception {


        Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "create data " + coll + " " + partition);


		Map<String, Object> objectMap = JsonUtil.toMap(data);

        // add partition info
		objectMap.put(Cosmos.getDefaultPartitionKey(), partition);

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		checkValidId(objectMap);

		var resource = RetryUtil.executeWithRetry( () -> client.createDocument(
                collectionLink,
                objectMap,
                requestOptions(partition),
                false
		).getResource());

		log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, resource.getId(), partition, getAccount());

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Id cannot contain "\t", "\r", "\n", or cosmosdb will create invalid data.
	 * @param objectMap
	 */
	static void checkValidId(Map<String, Object> objectMap) {
		if(objectMap == null){
			return;
		}
		var id = objectMap.getOrDefault("id", "").toString();
		checkValidId(id);
	}

	static void checkValidId(String id) {
		if(StringUtils.containsAny(id, "\t", "\n", "\r")){
			throw new IllegalArgumentException("id cannot contain \\t or \\n or \\r. id:" + id);
		}
	}

	/**
	 * Create a document using default partition
	 * @param coll collection name
	 * @param data data Object
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument create(String coll, Object data) throws Exception {
		return create(coll, data, coll);
	}


	/**
	 *
	 * @param coll collection name
	 * @param id id of the document
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Throw 404 Not Found Exception if object not exist
	 */
	public CosmosDocument read(String coll, String id, String partition) throws Exception {

		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		var resource = RetryUtil.executeWithRetry( () -> client.readDocument(documentLink, requestOptions(partition)).getResource());

		log.info("read Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Read a document by coll and id
	 * @param coll collection name
	 * @param id id of document
	 * @return CosmosDocument instance
	 * @throws Exception Throw 404 Not Found Exception if object not exist
	 */
	public CosmosDocument read(String coll, String id) throws Exception {
		return read(coll, id, coll);
	}

	/**
	 * Read a document by coll and id. Return null if object not exist
	 * @param coll collection name
	 * @param id id of document
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument readSuppressing404(String coll, String id, String partition) throws Exception {

		try {
			return read(coll, id, partition);
		} catch (Exception e) {
			if (Cosmos.isResourceNotFoundException(e)) {
				return null;
			}
			throw e;
		}
	}

	/**
	 * Read a document by coll and id. Return null if object not exist
	 * @param coll collection name
	 * @param id id of document
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument readSuppressing404(String coll, String id) throws Exception {

		return readSuppressing404(coll, id, coll);
	}

	/**
	 * Update existing data. if not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument update(String coll, Object data, String partition) throws Exception {
		
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "update data " + coll + " " + partition);

		var map = JsonUtil.toMap(data);
		var id = map.getOrDefault("id", "").toString();

		Checker.checkNotBlank(id, "id");
		checkValidId(id);

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		// add partition info
		map.put(Cosmos.getDefaultPartitionKey(), partition);

		var resource = RetryUtil.executeWithRetry( () -> client.replaceDocument(documentLink, map, requestOptions(partition)).getResource());

		log.info("updated Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}


	/**
	 * Update existing data. if not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param data data object
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument update(String coll, Object data) throws Exception {
		return update(coll, data, coll);
	}

	/**
	 * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param id id of document
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument updatePartial(String coll, String id, Object data, String partition)
			throws Exception {

		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "updatePartial data " + coll + " " + partition);

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		var origin = read(coll, id, partition).toMap();

		var newData = JsonUtil.toMap(data);
		// add partition info
		newData.put(Cosmos.getDefaultPartitionKey(), partition);

		// Object.assign(origin, newData)
		var merged = merge(origin, newData);

		checkValidId(merged);
		var resource = RetryUtil.executeWithRetry( () -> client.replaceDocument(documentLink, merged, requestOptions(partition)).getResource());

		log.info("updatePartial Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param id id of document
	 * @param data data object
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument updatePartial(String coll, String id, Object data) throws Exception {
		return updatePartial(coll, id, data, coll);
	}

	/**
	 * Update existing data. Create a new one if not exist. "id" field must be contained in data.
	 * @param coll collection name
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument upsert(String coll, Object data, String partition) throws Exception {

		var map = JsonUtil.toMap(data);
		var id = map.getOrDefault("id", "").toString();

		Checker.checkNotBlank(id, "id");
		checkValidId(id);
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "upsert data " + coll + " " + partition);

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		// add partition info
		map.put(Cosmos.getDefaultPartitionKey(), partition);

		var resource = RetryUtil.executeWithRetry( () -> client.upsertDocument(collectionLink, map, requestOptions(partition), true).getResource());

		log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Update existing data. Create a new one if not exist. "id" field must be contained in data.
	 * @param coll collection name
	 * @param data data object
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument upsert(String coll, Object data) throws Exception {
		return upsert(coll, data, coll);
	}

	/**
	 * Upsert data (Partial upsert supported. Only the 1st json hierarchy). if not
	 * exist, create the data. if already exist, update the data. "id" field must be
	 * contained in data.
	 *
	 * @param coll collection name
	 * @param id id of document
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument upsertPartial(String coll, String id, Object data, String partition)
			throws Exception {

		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "upsertPartial data " + coll + " " + partition);

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var originResource = readSuppressing404(coll, id, partition);
		var origin = originResource == null ? null : originResource.toMap();

		var newData = JsonUtil.toMap(data);
		// add partition info
		newData.put(Cosmos.getDefaultPartitionKey(), partition);

		var merged = origin == null ? newData : merge(origin, newData);

		checkValidId(merged);
		var resource = RetryUtil.executeWithRetry( () -> client.upsertDocument(collectionLink, merged, requestOptions(partition), true).getResource());

		log.info("upsertPartial Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Upsert data (Partial upsert supported. Only the 1st json hierarchy). if not
	 * exist, create the data. if already exist, update the data. "id" field must be
	 * contained in data.
	 *
	 * @param coll collection name
	 * @param id id of document
	 * @param data data object
	 * @return CosmosDocument instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocument upsertPartial(String coll, String id, Object data) throws Exception {
		return upsertPartial(coll, id, data, coll);
	}

	/**
	 * Delete a document. Do nothing if object not exist
	 * @param coll collection name
	 * @param id id of document
	 * @param partition partition name
	 * @return CosmosDatabase instance
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDatabase delete(String coll, String id, String partition) throws Exception {

		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(partition, "partition");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		try {
			RetryUtil.executeWithRetry( () -> client.deleteDocument(documentLink, requestOptions(partition)).getResource());
			log.info("deleted Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		} catch (Exception e) {
			if (Cosmos.isResourceNotFoundException(e)) {
				log.info("delete Document not exist. Ignored:{}, partition:{}, account:{}", documentLink, partition, getAccount());
				return this;
			}
			throw e;
		}
		return this;

	}

	/**
	 * Delete a document by selfLink. Do nothing if object not exist
	 * @param selfLink selfLink of a document
	 * @param partition partition name
	 * @return CosmosDatabase instance
	 * @throws Exception Cosmos client exception
	 */

	public CosmosDatabase deleteBySelfLink(String selfLink, String partition) throws Exception {

		Checker.checkNotBlank(selfLink, "selfLink");
		Checker.checkNotBlank(partition, "partition");

		try {
			RetryUtil.executeWithRetry( () -> client.deleteDocument(selfLink, requestOptions(partition)).getResource());
			log.info("deleted Document:{}, partition:{}, account:{}", selfLink, partition, getAccount());

		} catch (Exception e) {
			if (Cosmos.isResourceNotFoundException(e)) {
				log.info("delete Document not exist. Ignored:{}, partition:{}, account:{}", selfLink, partition, getAccount());
				return this;
			}
			throw e;
		}
		return this;

	}

	/**
	 * find data by condition
	 *
	 * {@code
	 *  var cond = Condition.filter(
	 *    "id>=", "id010", // id greater or equal to 'id010'
	 *    "lastName", "Banks" // last name equal to Banks
	 *  )
	 *  .order("lastName", "ASC") //optional order
	 *  .offset(0) //optional offset
	 *  .limit(100); //optional limit
	 *
	 *  var users = db.find("Collection1", cond, "Users").toList(User.class);
	 *
	 * }
	 *
	 * @param coll collection name
	 * @param cond condition to find
	 * @param partition partition name
	 * @throws Exception Cosmos client exception
	 * @return CosmosDocumentList
	 */

	public CosmosDocumentList find(String coll, Condition cond, String partition) throws Exception {
		// do a find without aggregate
		return find(coll, null, cond, partition);

	}

	/**
	 * A helper method to do find/aggregate by condition
	 *
	 * @param coll      collection name
	 * @param aggregate aggregate settings. null if no aggration needed.
	 * @param cond      condition to find
	 * @param partition partition name
	 * @return CosmosDocumentList
	 * @throws Exception Cosmos client exception
	 */
	CosmosDocumentList find(String coll, Aggregate aggregate, Condition cond, String partition) throws Exception {

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var feedOptions = new FeedOptions();
		if (cond.crossPartition) {
			feedOptions.setEnableCrossPartitionQuery(true);
		} else {
			feedOptions.setPartitionKey(new PartitionKey(partition));
		}

		var querySpec = cond.toQuerySpec(aggregate);

		var docs = RetryUtil.executeWithRetry(() -> client.queryDocuments(collectionLink, querySpec, feedOptions).getQueryIterable().toList());

		if (log.isInfoEnabled()) {
			log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
		}

		var jsonObjs = docs.stream().map(it -> it.toObject(JSONObject.class)).collect(Collectors.toList());

		return new CosmosDocumentList(jsonObjs);

	}

	/**
	 * find data by condition (partition is default to the same name as the coll or ignored when crossPartition is true)
	 * <p>
	 * {@code
	 * var cond = Condition.filter(
	 * "id>=", "id010", // id greater or equal to 'id010'
	 * "lastName", "Banks" // last name equal to Banks
	 * )
	 * .order("lastName", "ASC") //optional order
	 * .offset(0) //optional offset
	 * .limit(100); //optional limit
	 * <p>
	 * var users = db.find("Collection1", cond).toList(User.class);
	 * <p>
	 * }
	 *
	 * @param coll collection name
	 * @param cond condition to find
	 * @return CosmosDocumentList
	 * @throws Exception Cosmos client exception
	 */

	public CosmosDocumentList find(String coll, Condition cond) throws Exception {
		return find(coll, cond, coll);
	}


	/**
	 * do an aggregate query by Aggregate and Condition
	 *
	 * {@code
	 *
	 *  var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("location", "gender");
	 *  var result = db.aggregate("Collection1", aggregate, "Users").toMap();
	 *
	 * }
	 *
	 * @param coll collection name
	 * @param aggregate Aggregate function and groupBys
	 * @param partition partition name
	 * @throws Exception Cosmos client exception
	 * @return CosmosDocumentList
	 */
	public CosmosDocumentList aggregate(String coll, Aggregate aggregate, String partition) throws Exception {
		return aggregate(coll, aggregate, Condition.filter(), partition);
	}

	/**
	 * do an aggregate query by Aggregate and Condition
	 *
	 * {@code
	 *
	 *  var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("location", "gender");
	 *  var cond = Condition.filter(
	 *    "age>=", "20",
	 *  );
	 *
	 *  var result = db.aggregate("Collection1", aggregate, cond, "Users").toMap();
	 *
	 * }
	 *
	 * @param coll collection name
	 * @param aggregate Aggregate function and groupBys
	 * @param cond condition to find
	 * @param partition partition name
	 * @throws Exception Cosmos client exception
	 * @return CosmosDocumentList
	 */
	public CosmosDocumentList aggregate(String coll, Aggregate aggregate, Condition cond, String partition) throws Exception {
		return find(coll, aggregate, cond, partition);
	}

	/**
	 * do an aggregate query by Aggregate and Condition (partition default to the same as coll or ignored when crossPartition is true)
	 * <p>
	 * {@code
	 * <p>
	 * var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("location", "gender");
	 * var cond = Condition.filter(
	 * "age>=", "20",
	 * );
	 * <p>
	 * var result = db.aggregate("Collection1", aggregate, cond).toMap();
	 * <p>
	 * }
	 *
	 * @param coll      collection name
	 * @param aggregate Aggregate function and groupBys
	 * @param cond      condition to find
	 * @return CosmosDocumentList
	 * @throws Exception Cosmos client exception
	 */
	public CosmosDocumentList aggregate(String coll, Aggregate aggregate, Condition cond) throws Exception {
		return find(coll, aggregate, cond, coll);
	}

	/**
	 * count data by condition
	 *
	 * {@code
	 *  var cond = Condition.filter(
	 *    "id>=", "id010", // id greater or equal to 'id010'
	 *    "lastName", "Banks" // last name equal to Banks
	 *  );
	 *
	 *  var count = db.count("Collection1", cond, "Users");
	 *
	 * }
	 *
	 * @param coll collection name
	 * @param cond condition to find
	 * @param partition partition name
	 * @throws Exception Cosmos client exception
	 * @return count of documents
	 */

	public int count(String coll, Condition cond, String partition) throws Exception {

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var options = new FeedOptions();
		options.setPartitionKey(new PartitionKey(partition));

		var querySpec = cond.toQuerySpecForCount();

		var docs = RetryUtil.executeWithRetry( () -> client.queryDocuments(collectionLink, querySpec, options).getQueryIterable().toList());

		if(log.isInfoEnabled()){
			log.info("count Document:{}, cond:{}, partition:{}, account:{}", coll, cond, partition, getAccount());
		}

		return docs.get(0).getInt("$1");

	}

	RequestOptions requestOptions(String partition) {
		var options = new RequestOptions();
		options.setPartitionKey(new PartitionKey(partition));
		return options;
    }

	/**
	 *
	 * Object.assign(m1, m2) in javascript.
	 *
	 * @param m1
	 * @param m2
	 * @return
	 */
	static Map<String, Object> merge(Map<String, Object> m1, Map<String, Object> m2) {
		m1.putAll(m2);
		return m1;
	}

	/**
	 * Get cosmos db account id associated with this instance.
	 * @return
	 * @throws Exception Cosmos client exception
	 */
	String getAccount() throws Exception {
		if(StringUtils.isNotEmpty(this.account)){
			return this.account;
		}
		this.account = Cosmos.getAccount(this.client);
		return this.account;
	}

	/**
	 * Get cosmos db account instance associated with this instance.
	 * @return cosmosAccount
	 */
	public Cosmos getCosmosAccount() {
		return this.cosmosAccount;
	}

	/**
	 * Get cosmos database name associated with this instance.
	 * @return database name
	 */
	public String getDatabaseName() {
		return this.db;
	}

}
