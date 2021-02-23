package io.github.thunderz99.cosmos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;

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

	CosmosClient client;

	CosmosDatabase(CosmosClient client, String db, String account) {
		this.client = client;
		this.db = db;
		this.account = account;
	}


	/**
	 * Create a document
	 * @param coll collection name
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 */
	public CosmosDocument create(String coll, Object data, String partition) {


        Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");
		Checker.checkNotNull(data, "create data " + coll + " " + partition);


		Map<String, Object> objectMap = JsonUtil.toMap(data);

        // add partition info
		objectMap.put(Cosmos.getDefaultPartitionKey(), partition);

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		checkValidId(objectMap);

		var container = client.getDatabase(db).getContainer(coll);

		var item = container.createItem(objectMap, new PartitionKey(partition), new CosmosItemRequestOptions());

		var map = item.getItem();

		log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, map.getOrDefault("id", ""), partition, account);

		return new CosmosDocument(map);
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
	 */
	public CosmosDocument create(String coll, Object data) {
		return create(coll, data, coll);
	}


	/**
	 *
	 * @param coll collection name
	 * @param id id of the document
	 * @param partition partition name
	 * @return CosmosDocument instance
	 */
	public CosmosDocument read(String coll, String id, String partition) {

		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(partition, "partition");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);

		var container = client.getDatabase(db).getContainer(coll);
		var sampleMap = new LinkedHashMap<String, Object>();
		var item = container.readItem(id, new PartitionKey(partition), new CosmosItemRequestOptions(), sampleMap.getClass());

		log.info("read Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		return new CosmosDocument(item.getItem());
	}

	/**
	 * Read a document by coll and id
	 * @param coll collection name
	 * @param id id of document
	 * @return CosmosDocument instance
	 */
	public CosmosDocument read(String coll, String id) {
		return read(coll, id, coll);
	}

	/**
	 * Read a document by coll and id. Return null if object not exist
	 * @param coll collection name
	 * @param id id of document
	 * @param partition partition name
	 * @return CosmosDocument instance
	 */
	public CosmosDocument readSuppressing404(String coll, String id, String partition) {

		try {
			return read(coll, id, partition);
		} catch (RuntimeException e) {
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
	 */
	public CosmosDocument readSuppressing404(String coll, String id) {

		return readSuppressing404(coll, id, coll);
	}

	/**
	 * Update existing data. if not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 */
	public CosmosDocument update(String coll, Object data, String partition) {
		
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

		var container = client.getDatabase(db).getContainer(coll);

		var item = container.replaceItem(map, id, new PartitionKey(partition), new CosmosItemRequestOptions());

		log.info("updated Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		return new CosmosDocument(item.getItem());
	}


	/**
	 * Update existing data. if not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param data data object
	 * @return CosmosDocument instance
	 */
	public CosmosDocument update(String coll, Object data) {
		return update(coll, data, coll);
	}

	/**
	 * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param id id of document
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 */
	public CosmosDocument updatePartial(String coll, String id, Object data, String partition) {

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

		var container = client.getDatabase(db).getContainer(coll);
		var item = container.replaceItem(merged, id, new PartitionKey(partition), new CosmosItemRequestOptions());

		log.info("updatePartial Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		return new CosmosDocument(item.getItem());
	}

	/**
	 * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
	 * @param coll collection name
	 * @param id id of document
	 * @param data data object
	 * @return CosmosDocument instance
	 */
	public CosmosDocument updatePartial(String coll, String id, Object data) {
		return updatePartial(coll, id, data, coll);
	}

	/**
	 * Update existing data. Create a new one if not exist. "id" field must be contained in data.
	 * @param coll collection name
	 * @param data data object
	 * @param partition partition name
	 * @return CosmosDocument instance
	 */
	public CosmosDocument upsert(String coll, Object data, String partition) {

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

		var container = client.getDatabase(db).getContainer(coll);
		var item = container.upsertItem(map, new PartitionKey(partition), new CosmosItemRequestOptions());

		log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

		return new CosmosDocument(item.getItem());
	}

	/**
	 * Update existing data. Create a new one if not exist. "id" field must be contained in data.
	 * @param coll collection name
	 * @param data data object
	 * @return CosmosDocument instance
	 */
	public CosmosDocument upsert(String coll, Object data) {
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
	 */
	public CosmosDocument upsertPartial(String coll, String id, Object data, String partition) {

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

		var container = client.getDatabase(db).getContainer(coll);
		var item = container.replaceItem(merged, id, new PartitionKey(partition), new CosmosItemRequestOptions());

		log.info("upsertPartial Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

		return new CosmosDocument(item.getItem());
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
	 */
	public CosmosDocument upsertPartial(String coll, String id, Object data) {
		return upsertPartial(coll, id, data, coll);
	}

	/**
	 * Delete a document. Do nothing if object not exist
	 * @param coll collection name
	 * @param id id of document
	 * @param partition partition name
	 * @return CosmosDatabase instance
	 */
	public CosmosDatabase delete(String coll, String id, String partition) {

		Checker.checkNotBlank(coll, "coll");
		Checker.checkNotBlank(id, "id");
		Checker.checkNotBlank(partition, "partition");

		var documentLink = Cosmos.getDocumentLink(db, coll, id);
		var container = client.getDatabase(db).getContainer(coll);

		try {
			container.deleteItem(id, new PartitionKey(partition), new CosmosItemRequestOptions());
			log.info("deleted Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

		} catch (RuntimeException e) {
			if (Cosmos.isResourceNotFoundException(e)) {
				log.info("delete Document not exist. Ignored:{}, partition:{}, account:{}", documentLink, partition, getAccount());
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
	 *  var users = db.find("Collection1", cond).toList(User.class);
	 *
	 * }
	 *
	 * @param coll collection name
	 * @param cond condition to find
	 * @param partition partition name
	 * @return CosmosDocumentList
	 */

	public CosmosDocumentList find(String coll, Condition cond, String partition) {

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var querySpec = cond.toQuerySpec();

		var container = client.getDatabase(db).getContainer(coll);

		var options = new CosmosQueryRequestOptions();
		options.setPartitionKey(new PartitionKey(partition));

		var sampleMap = (Map<String, Object>) new LinkedHashMap<String, Object>();
		var docs = container.queryItems(querySpec, options,  sampleMap.getClass()).stream().collect(Collectors.toList());

		if(log.isInfoEnabled()){
			log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, partition, getAccount());
		}

		return new CosmosDocumentList((List<Map<String, Object>>) docs);

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
	 * @return count of documents
	 */

	public int count(String coll, Condition cond, String partition) {

		var collectionLink = Cosmos.getCollectionLink(db, coll);

		var options = new CosmosQueryRequestOptions();
		options.setPartitionKey(new PartitionKey(partition));

		var querySpec = cond.toQuerySpecForCount();

		var container = client.getDatabase(db).getContainer(coll);

		var sampleMap = (Map<String, Object>) new LinkedHashMap<String, Object>();
		var docs = container.queryItems(querySpec, options, sampleMap.getClass()).stream().collect(Collectors.toList());

		if(log.isInfoEnabled()){
			log.info("count Document:{}, cond:{}, partition:{}, account:{}", coll, cond, partition, getAccount());
		}


		return Integer.parseInt(docs.get(0).getOrDefault("$1", "0").toString());

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
	 */
	String getAccount() {
		return this.account;
	}

}
