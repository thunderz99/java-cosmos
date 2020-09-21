package io.github.thunderz99.cosmos;

import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.FeedOptions;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;

public class CosmosDatabase {

	private static Logger log = LoggerFactory.getLogger(CosmosDatabase.class);

	String db;

	DocumentClient client;

	CosmosDatabase(DocumentClient client, String db) {
		this.client = client;
		this.db = db;

	}


	public CosmosDocument create(String coll, Object data, String partition) throws DocumentClientException {


        Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "create data " + coll + " " + partition);


		Map<String, Object> objectMap = JsonUtil.toMap(data);

        // add partition info
		objectMap.put(Cosmos.getDefaultPartitionKey(), partition);

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var resource = client.createDocument(
                collectionLink,
                objectMap,
                requestOptions(partition),
                false
		).getResource();

		log.info("created Document: {}/docs/{}, partition:{}", coll, resource.getId(), partition);

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	public CosmosDocument create(String coll, Object data) throws DocumentClientException {
		return create(coll, data, coll);
	}


	/**
	 * Throw 404 Not Found Exception if object not exist
	 */
	public CosmosDocument read(String coll, String id, String partition) throws DocumentClientException {

		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		var resource = client.readDocument(documentLink, requestOptions(partition)).getResource();

		log.info("read Document: {}/docs/{}, partition:{}", coll, id, partition);

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Throw 404 Not Found Exception if object not exist
	 */
	public CosmosDocument read(String coll, String id) throws DocumentClientException {
		return read(coll, id, coll);
	}

	/**
	 * Return null if object not exist
	 */
	public CosmosDocument readSuppressing404(String coll, String id, String partition) throws DocumentClientException {

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
	 * Return null if object not exist
	 */
	public CosmosDocument readSuppressing404(String coll, String id) throws DocumentClientException {

		return readSuppressing404(coll, id, coll);
	}

	/**
	 * Update existing data. if not exist, throw Not Found Exception.
	 */
	public CosmosDocument update(String coll, Object data, String partition) throws DocumentClientException {
		
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "update data " + coll + " " + partition);

		var map = JsonUtil.toMap(data);
		var id = map.getOrDefault("id", "").toString();

		Checker.checkNotBlank(id, "id");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		// add partition info
		map.put(Cosmos.getDefaultPartitionKey(), partition);

		var resource = client.replaceDocument(documentLink, map, requestOptions(partition)).getResource();

		log.info("updated Document: {}/docs/{}, partition:{}", coll, id, partition);

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Update existing data. if not exist, throw Not Found Exception.
	 */
	public CosmosDocument update(String coll, Object data) throws DocumentClientException {
		return update(coll, data, coll);
	}

	/**
	 * Update existing data. Partial update supported(Only 1st json hierarchy
	 * supported). If not exist, throw Not Found Exception.
	 */
	public CosmosDocument updatePartial(String coll, String id, Object data, String partition)
			throws DocumentClientException {

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

		var resource = client.replaceDocument(documentLink, merged, requestOptions(partition)).getResource();

		log.info("updatePartial Document: {}/docs/{}, partition:{}", coll, id, partition);

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * Update existing data. Partial update supported(Only 1st json hierarchy
	 * supported). If not exist, throw Not Found Exception.
	 */
	public CosmosDocument updatePartial(String coll, String id, Object data) throws DocumentClientException {
		return updatePartial(coll, id, data, coll);
	}

	/**
	 * upsert data.if not exist, create the data. if already exist, update the data.
	 * "id" field must be contained in data.
	 *
	 * @param coll
	 * @param data
	 * @param partition
	 * @return
	 * @throws DocumentClientException
	 */
	public CosmosDocument upsert(String coll, Object data, String partition) throws DocumentClientException {

		var map = JsonUtil.toMap(data);
		var id = map.getOrDefault("id", "").toString();

		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "upsert data " + coll + " " + partition);

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		// add partition info
		map.put(Cosmos.getDefaultPartitionKey(), partition);

		var resource = client.upsertDocument(collectionLink, map, requestOptions(partition), true).getResource();

		log.info("upsert Document: {}/docs/{}, partition:{}", coll, id, partition);

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * upsert data. "id" field must be contained in data.
	 *
	 * @param coll
	 * @param data
	 * @param partition
	 * @return
	 * @throws DocumentClientException
	 */
	public CosmosDocument upsert(String coll, Object data) throws DocumentClientException {
		return upsert(coll, data, coll);
	}

	/**
	 * upsert data (Partial upsert supported. Only the 1st json hierarchy). if not
	 * exist, create the data. if already exist, update the data. "id" field must be
	 * contained in data.
	 *
	 * @param coll
	 * @param data
	 * @param partition
	 * @return
	 * @throws DocumentClientException
	 */
	public CosmosDocument upsertPartial(String coll, String id, Object data, String partition)
			throws DocumentClientException {

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

		var resource = client.upsertDocument(collectionLink, merged, requestOptions(partition), true).getResource();

		log.info("upsertPartial Document: {}/docs/{}, partition:{}", coll, id, partition);

		return new CosmosDocument(resource.toObject(JSONObject.class));
	}

	/**
	 * upsert data (Partial upsert supported. Only the 1st json hierarchy). if not
	 * exist, create the data. if already exist, update the data. "id" field must be
	 * contained in data.
	 *
	 * @param coll
	 * @param data
	 * @return
	 * @throws DocumentClientException
	 */
	public CosmosDocument upsertPartial(String coll, String id, Object data) throws DocumentClientException {
		return upsertPartial(coll, id, data, coll);
	}

	/**
	 * Do nothing if object not exist
	 */
	public CosmosDatabase delete(String coll, String id, String partition) throws DocumentClientException {

		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(partition, "partition");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		try {
			client.deleteDocument(documentLink, requestOptions(partition)).getResource();
			log.info("deleted Document: {}/docs/{}, partition:{}", coll, id, partition);

		} catch (Exception e) {
			if (Cosmos.isResourceNotFoundException(e)) {
				log.info("delete Document not exist. Ignored: {}/docs/{}, partition:{}", coll, id, partition);
			}
		}
		return this;

	}

	/**
	 * find data by condition
	 *
	 * <code>
	 *  var cond = Condition.filter(
	 *    "id>=", "id010", // id greater or equal to 'id010'
	 *    "lastName", "Banks" // last name equal to Banks
	 *  )
	 *  .order("lastName", "ASC") //optional order
	 *  .offset(0) //optional offset
	 *  .limit(100); //optional limit
	 *
	 *  var users = db.find("Collection1", cond).toList(User.class);
	 *
	 * </code>
	 *
	 *
	 * @param coll
	 * @param cond
	 * @param partition
	 * @return
	 */

	public CosmosDocumentList find(String coll, Condition cond, String partition) {

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var options = new FeedOptions();
		options.setPartitionKey(new PartitionKey(partition));

		var querySpec = cond.toQuerySpec();

		var docs = client.queryDocuments(collectionLink, querySpec, options).getQueryIterable().toList();

		if(log.isInfoEnabled()){
			log.info(String.format("find Document: collection: %s, cond: %s, partition:%s", coll, cond, partition));
		}

		var jsonObjs = docs.stream().map(it -> it.toObject(JSONObject.class)).collect(Collectors.toList());

		return new CosmosDocumentList(jsonObjs);

	}

	/**
	 * count data by condition
	 *
	 * <code>
	 *  var cond = Condition.filter(
	 *    "id>=", "id010", // id greater or equal to 'id010'
	 *    "lastName", "Banks" // last name equal to Banks
	 *  );
	 *
	 *  var count = db.count("Collection1", cond, "Users");
	 *
	 * </code>
	 *
	 *
	 * @param coll
	 * @param cond
	 * @param partition
	 * @return
	 */

	public int count(String coll, Condition cond, String partition) {

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var options = new FeedOptions();
		options.setPartitionKey(new PartitionKey(partition));

		var querySpec = cond.toQuerySpecForCount();

		var docs = client.queryDocuments(collectionLink, querySpec, options).getQueryIterable().toList();

		if(log.isInfoEnabled()){
			log.info(String.format("count Document: collection: %s, cond: %s, partition:%s", coll, cond, partition));
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

}
