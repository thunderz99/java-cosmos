package io.github.thunderz99.cosmos;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.RetryUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    Cosmos cosmosAccount;

    CosmosDatabase(CosmosClient client, String db, Cosmos cosmosAccount) {
        this.client = client;
        this.db = db;
        this.cosmosAccount = cosmosAccount;
        this.account = this.cosmosAccount.getAccount();
    }


    /**
     * Create a document
     *
     * @param con       container name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos Client Exception
     */
    public CosmosDocument create(String con, Object data, String partition) throws Exception {


        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "create data " + con + " " + partition);


        Map<String, Object> objectMap = JsonUtil.toMap(data);

        // add partition info
        objectMap.put(Cosmos.getDefaultPartitionKey(), partition);

        var collectionLink = Cosmos.getCollectionLink(db, con);

        checkValidId(objectMap);

        var container = client.getDatabase(db).getContainer(con);

        var resource = RetryUtil.executeWithRetry(() -> container.createItem(objectMap, new PartitionKey(partition), new CosmosItemRequestOptions()));

        var map = resource.getItem();

        log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, map.getOrDefault("id", ""), partition, getAccount());

        return new CosmosDocument(map);
    }

    /**
     * Id cannot contain "\t", "\r", "\n", or cosmosdb will create invalid data.
     *
     * @param objectMap
     */
    static void checkValidId(Map<String, Object> objectMap) {
        if (objectMap == null) {
            return;
        }
        var id = objectMap.getOrDefault("id", "").toString();
        checkValidId(id);
    }

    static void checkValidId(String id) {
        if (StringUtils.containsAny(id, "\t", "\n", "\r")) {
            throw new IllegalArgumentException("id cannot contain \\t or \\n or \\r. id:" + id);
        }
    }

    /**
     * Create a document using default partition
     *
     * @param con  container name
     * @param data data Object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument create(String con, Object data) throws Exception {
        return create(con, data, con);
    }


    /**
     * @param con       container name
     * @param id        id of the document
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    public CosmosDocument read(String con, String id, String partition) throws Exception {

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(partition, "partition");

        var documentLink = Cosmos.getDocumentLink(db, con, id);

        var container = client.getDatabase(db).getContainer(con);

        var sampleMap = new LinkedHashMap<String, Object>();
        var item = RetryUtil.executeWithRetry(() -> container.readItem(id, new PartitionKey(partition), new CosmosItemRequestOptions(), sampleMap.getClass()));

        log.info("read Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(item.getItem());
    }

    /**
     * Read a document by con and id
     *
     * @param con container name
     * @param id  id of document
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    public CosmosDocument read(String con, String id) throws Exception {
        return read(con, id, con);
    }

    /**
     * Read a document by con and id. Return null if object not exist
     *
     * @param con       container name
     * @param id        id of document
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument readSuppressing404(String con, String id, String partition) throws Exception {

        try {
            return read(con, id, partition);
        } catch (CosmosException e) {
            if (Cosmos.isResourceNotFoundException(e)) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Read a document by con and id. Return null if object not exist
     *
     * @param con container name
     * @param id  id of document
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument readSuppressing404(String con, String id) throws Exception {

        return readSuppressing404(con, id, con);
    }

    /**
     * Update existing data. if not exist, throw Not Found Exception.
     *
     * @param con       container name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument update(String con, Object data, String partition) throws Exception {

        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "update data " + con + " " + partition);

        var map = JsonUtil.toMap(data);
        var id = map.getOrDefault("id", "").toString();

        Checker.checkNotBlank(id, "id");
        checkValidId(id);

        var documentLink = Cosmos.getDocumentLink(db, con, id);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

        var container = client.getDatabase(db).getContainer(con);

        var item = RetryUtil.executeWithRetry(() -> container.replaceItem(map, id, new PartitionKey(partition), new CosmosItemRequestOptions()));

        log.info("updated Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(item.getItem());
    }


    /**
     * Update existing data. if not exist, throw Not Found Exception.
     *
     * @param con  container name
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument update(String con, Object data) throws Exception {
        return update(con, data, con);
    }

    /**
     * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
     *
     * @param con       container name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument updatePartial(String con, String id, Object data, String partition)
            throws Exception {

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "updatePartial data " + con + " " + partition);

        var documentLink = Cosmos.getDocumentLink(db, con, id);

        var origin = read(con, id, partition).toMap();

        var newData = JsonUtil.toMap(data);
        // add partition info
        newData.put(Cosmos.getDefaultPartitionKey(), partition);

        // Object.assign(origin, newData)
        var merged = merge(origin, newData);

        checkValidId(merged);

        var container = client.getDatabase(db).getContainer(con);
        var item = RetryUtil.executeWithRetry(() -> container.replaceItem(merged, id, new PartitionKey(partition), new CosmosItemRequestOptions()));

        log.info("updatePartial Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(item.getItem());

    }

    /**
     * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
     *
     * @param con  container name
     * @param id   id of document
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument updatePartial(String con, String id, Object data) throws Exception {
        return updatePartial(con, id, data, con);
    }

    /**
     * Update existing data. Create a new one if not exist. "id" field must be contained in data.
     *
     * @param con       container name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument upsert(String con, Object data, String partition) throws Exception {

        var map = JsonUtil.toMap(data);
        var id = map.getOrDefault("id", "").toString();

        Checker.checkNotBlank(id, "id");
        checkValidId(id);
        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "upsert data " + con + " " + partition);

        var collectionLink = Cosmos.getCollectionLink(db, con);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

        var container = client.getDatabase(db).getContainer(con);
        var item = RetryUtil.executeWithRetry(() -> container.upsertItem(map, new PartitionKey(partition), new CosmosItemRequestOptions()));

        log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

        return new CosmosDocument(item.getItem());
    }

    /**
     * Update existing data. Create a new one if not exist. "id" field must be contained in data.
     *
     * @param con  container name
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument upsert(String con, Object data) throws Exception {
        return upsert(con, data, con);
    }

    /**
     * Upsert data (Partial upsert supported. Only the 1st json hierarchy). if not
     * exist, create the data. if already exist, update the data. "id" field must be
     * contained in data.
     *
     * @param con       container name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument upsertPartial(String con, String id, Object data, String partition)
            throws Exception {

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "upsertPartial data " + con + " " + partition);

        var collectionLink = Cosmos.getCollectionLink(db, con);

        var originResource = readSuppressing404(con, id, partition);
        var origin = originResource == null ? null : originResource.toMap();

        var newData = JsonUtil.toMap(data);
        // add partition info
        newData.put(Cosmos.getDefaultPartitionKey(), partition);

        var merged = origin == null ? newData : merge(origin, newData);

        checkValidId(merged);

        var container = client.getDatabase(db).getContainer(con);
        var item = RetryUtil.executeWithRetry(() -> container.upsertItem(merged, new PartitionKey(partition), new CosmosItemRequestOptions()));

        log.info("upsertPartial Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

        return new CosmosDocument(item.getItem());
    }

    /**
     * Upsert data (Partial upsert supported. Only the 1st json hierarchy). if not
     * exist, create the data. if already exist, update the data. "id" field must be
     * contained in data.
     *
     * @param con  container name
     * @param id   id of document
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument upsertPartial(String con, String id, Object data) throws Exception {
        return upsertPartial(con, id, data, con);
    }

    /**
     * Delete a document. Do nothing if object not exist
     *
     * @param con       container name
     * @param id        id of document
     * @param partition partition name
     * @return CosmosDatabase instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDatabase delete(String con, String id, String partition) throws Exception {

        Checker.checkNotBlank(con, "con");
        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(partition, "partition");

        var documentLink = Cosmos.getDocumentLink(db, con, id);
        var container = client.getDatabase(db).getContainer(con);

        try {
            RetryUtil.executeWithRetry(() -> container.deleteItem(id, new PartitionKey(partition), new CosmosItemRequestOptions()));
            log.info("deleted Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        } catch (CosmosException e) {
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
     * var users = db.find("Collection1", cond, "Users").toList(User.class);
     * <p>
     * }
     *
     * @param con       container name
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */

    public CosmosDocumentList find(String con, Condition cond, String partition) throws Exception {
        // do a find without aggregate
        return find(con, null, cond, partition);

    }

    /**
     * A helper method to do find/aggregate by condition
     *
     * @param con       container name
     * @param aggregate aggregate settings. null if no aggration needed.
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    CosmosDocumentList find(String con, Aggregate aggregate, Condition cond, String partition) throws Exception {

        var options = new CosmosQueryRequestOptions();
        if (cond.crossPartition) {
            // do not set partition key
        } else {
            options.setPartitionKey(new PartitionKey(partition));
        }


        var collectionLink = Cosmos.getCollectionLink(db, con);

        var querySpec = cond.toQuerySpec(aggregate);
        var container = client.getDatabase(db).getContainer(con);

        var sampleMap = (Map<String, Object>) new LinkedHashMap<String, Object>();

        var docs = RetryUtil.executeWithRetry(() -> container.queryItems(querySpec, options, sampleMap.getClass()).stream().collect(Collectors.toList()));


        if (log.isInfoEnabled()) {
            log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
        }

        return new CosmosDocumentList((List<Map<String, Object>>) docs);

    }

    /**
     * find data by condition (partition is default to the same name as the con or ignored when crossPartition is true)
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
     * @param con  container name
     * @param cond condition to find
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */

    public CosmosDocumentList find(String con, Condition cond) throws Exception {
        return find(con, cond, con);
    }


    /**
     * do an aggregate query by Aggregate and Condition
     * <p>
     * {@code
     * <p>
     * var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("location", "gender");
     * var result = db.aggregate("Collection1", aggregate, "Users").toMap();
     * <p>
     * }
     *
     * @param con       container name
     * @param aggregate Aggregate function and groupBys
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentList aggregate(String con, Aggregate aggregate, String partition) throws Exception {
        return aggregate(con, aggregate, Condition.filter(), partition);
    }

    /**
     * do an aggregate query by Aggregate and Condition
     * <p>
     * {@code
     * <p>
     * var aggregate = Aggregate.function("COUNT(1) AS facetCount").groupBy("location", "gender");
     * var cond = Condition.filter(
     * "age>=", "20",
     * );
     * <p>
     * var result = db.aggregate("Collection1", aggregate, cond, "Users").toMap();
     * <p>
     * }
     *
     * @param con       container name
     * @param aggregate Aggregate function and groupBys
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentList aggregate(String con, Aggregate aggregate, Condition cond, String partition) throws Exception {
        return find(con, aggregate, cond, partition);
    }

    /**
     * do an aggregate query by Aggregate and Condition (partition default to the same as con or ignored when crossPartition is true)
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
     * @param con       container name
     * @param aggregate Aggregate function and groupBys
     * @param cond      condition to find
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentList aggregate(String con, Aggregate aggregate, Condition cond) throws Exception {
        return find(con, aggregate, cond, con);
    }

    /**
     * count data by condition
     * <p>
     * {@code
     * var cond = Condition.filter(
     * "id>=", "id010", // id greater or equal to 'id010'
     * "lastName", "Banks" // last name equal to Banks
     * );
     * <p>
     * var count = db.count("Collection1", cond, "Users");
     * <p>
     * }
     *
     * @param con       container name
     * @param cond      condition to find
     * @param partition partition name
     * @return count of documents
     * @throws Exception Cosmos client exception
     */

    public int count(String con, Condition cond, String partition) throws Exception {

        var options = new CosmosQueryRequestOptions();
        options.setPartitionKey(new PartitionKey(partition));

        var querySpec = cond.toQuerySpecForCount();

        var container = client.getDatabase(db).getContainer(con);

        var sampleMap = (Map<String, Object>) new LinkedHashMap<String, Object>();
        var docs = container.queryItems(querySpec, options, sampleMap.getClass()).stream().collect(Collectors.toList());

        if (log.isInfoEnabled()) {
            log.info("count Document:{}, cond:{}, partition:{}, account:{}", con, cond, partition, getAccount());
        }

        return Integer.parseInt(docs.get(0).getOrDefault("$1", "0").toString());

    }

    /**
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
     *
     * @return
     * @throws Exception Cosmos client exception
     */
    String getAccount() throws Exception {
        return this.account;
    }

    /**
     * Get cosmos db account instance associated with this instance.
     *
     * @return cosmosAccount
     */
    public Cosmos getCosmosAccount() {
        return this.cosmosAccount;
    }

    /**
     * Get cosmos database name associated with this instance.
     *
     * @return database name
     */
    public String getDatabaseName() {
        return this.db;
    }

}
