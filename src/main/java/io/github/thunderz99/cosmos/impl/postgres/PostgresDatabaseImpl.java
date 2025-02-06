package io.github.thunderz99.cosmos.impl.postgres;

import com.google.common.base.Preconditions;
import com.mongodb.client.AggregateIterable;
import com.zaxxer.hikari.HikariDataSource;
import io.github.thunderz99.cosmos.*;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.impl.postgres.util.PGAggregateUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.PGConditionUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.TTLUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.TableUtil;
import io.github.thunderz99.cosmos.util.*;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;

/**
 * Class representing a postgres schema instance.
 *
 * <p>
 * Can do document's CRUD and find.
 * </p>
 */
public class PostgresDatabaseImpl implements CosmosDatabase {

    public static final String DEFAULT_SCHEMA = "public";
    private static Logger log = LoggerFactory.getLogger(PostgresDatabaseImpl.class);

    static final int MAX_BATCH_NUMBER_OF_OPERATION = 100;

    /**
     * field automatically added to contain the expiration timestamp
     */
    public static final String EXPIRE_AT = "_expireAt";

    String db;
    HikariDataSource dataSource;

    Cosmos cosmosAccount;

    public PostgresDatabaseImpl(Cosmos cosmosAccount, String db) {
        this.cosmosAccount = cosmosAccount;
        this.db = StringUtils.isEmpty(db) ? DEFAULT_SCHEMA : db;
        if (cosmosAccount instanceof PostgresImpl) {
            this.dataSource = ((PostgresImpl) cosmosAccount).getDataSource();
        }

    }

    /**
     * Create a table representing a partition, if not exist. This table will have the standard table definition for java-cosmos(id, data)
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public String createTableIfNotExists(String schemaName, String tableName) throws Exception {

        try(var conn = dataSource.getConnection()) {
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);
            return tableName;
        }
    }

    /**
     * Drop a table representing a partition, if exists.
     *
     * @param tableName
     * @throws Exception
     */
    public void dropTableIfExists(String schemaName, String tableName) throws Exception {

        try(var conn = dataSource.getConnection()) {
            TableUtil.dropTableIfExists(conn, schemaName, tableName);
        }
    }

    /**
     * In postgres implementation, we map coll to schemaName
     * @return
     */
    public String getSchemaName(){
        return db;
    }

    /**
     * Create a document
     *
     * @param coll      collection name(use collection name for database for mongodb)
     * @param data      data object
     * @param partition partition name(use partition name for collection for mongodb)
     * @return CosmosDocument instance
     * @throws Exception Cosmos Client Exception
     */
    public CosmosDocument create(String coll, Object data, String partition) throws Exception {


        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "create data " + coll + " " + partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        Map<String, Object> map = JsonUtil.toMap(data);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

        // set id(for java-cosmos) and _id(for mongo) before insert
        var id = addId4Postgres(map);

        // add timestamp field "_ts"
        addTimestamp(map);

        // add _expireAt if ttl is set
        addExpireAt(map);

        // add etag for optimistic lock if enabled
        addEtag4(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        PostgresRecord record = null;
        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            record = TableUtil.insertRecord(conn, coll, partition, new PostgresRecord(id, map));
        }

        if (log.isInfoEnabled()){
            log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, record.id, partition, getAccount());
        }

        return getCosmosDocument(record);
    }

    /**
     * set "id" correctly, and generate an uuid if id is empty.
     *
     * @param objectMap
     */
    static String addId4Postgres(Map<String, Object> objectMap) {
        var id = objectMap.getOrDefault("id", UUID.randomUUID()).toString();
        checkValidId(id);
        objectMap.put("id", id);
        return id;
    }

    /**
     * add "_expireAt" field automatically if expireAtEnabled is true, and "ttl" has int value
     *
     * @param objectMap
     * @return expireAt Date. or null if not set.
     */
    Date addExpireAt(Map<String, Object> objectMap) {

        var account = (PostgresImpl) this.getCosmosAccount();
        if (!account.expireAtEnabled) {
            return null;
        }

        var ttlObj = objectMap.get("ttl");
        if (ttlObj == null) {
            return null;
        }

        if (!(ttlObj instanceof Integer)) {
            return null;
        }

        var ttl = (Integer) ttlObj;

        // Current time + ttl in milliseconds. use long because this will be possibly larger than Integer.MAX_VALUE
        var expireAt = new Date(System.currentTimeMillis() + 1000L * ttl);

        objectMap.put(EXPIRE_AT, expireAt);

        return expireAt;
    }

    /**
     * add "_etag" field automatically if etagEnabled is true
     *
     * @param objectMap
     * @return etag string value(uuid). or null if not set.
     */
    String addEtag4(Map<String, Object> objectMap) {

        var account = (PostgresImpl) this.getCosmosAccount();
        if (!account.etagEnabled) {
            return null;
        }

        var etag = UUID.randomUUID().toString();
        objectMap.put(TableUtil.ETAG, etag);

        return etag;
    }

    static String getId(Object object) {
        String id;
        if (object instanceof String) {
            id = (String) object;
        } else if (object instanceof Map map){
            id = map.getOrDefault("id", "").toString();
        } else {
            var map = JsonUtil.toMap(object);
            id = map.getOrDefault("id", "").toString();
        }
        return id;
    }


    static void checkValidId(List<?> data) {
        for (Object datum : data) {
            checkValidId(getId(datum));
        }
    }

    static void checkValidId(String id) {
        if (StringUtils.containsAny(id, "\t", "\n", "\r", "/")) {
            throw new IllegalArgumentException("id cannot contain \\t or \\n or \\r or /. id:" + id);
        }
    }


    /**
     * @param coll      collection name(used as mongodb database)
     * @param id        id of the document
     * @param partition partition name(used as mongodb collection)
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    public CosmosDocument read(String coll, String id, String partition) throws Exception {
       var ret = readSuppressing404(coll, id, partition);
        if (ret == null) {
            throw new CosmosException(404, "404", "Resource Not Found. code: NotFound");
        }
        return ret;
    }

    /**
     * process precision of timestamp and get CosmosDucment instance from response
     *
     * @param record
     * @return cosmos document
     */
    static CosmosDocument getCosmosDocument(PostgresRecord record) {
        record.data.put(TableUtil.ID, record.id);
        TimestampUtil.processTimestampPrecision(record.data);
        convertExpireAt2Date(record.data);
        return new CosmosDocument(record.data);
    }

    static void convertExpireAt2Date(Map<String, Object> map) {
        if (MapUtils.isEmpty(map)) {
            return;
        }

        var expireAt = map.get(EXPIRE_AT);

        if (expireAt == null) {
            // do nothing
            return;
        }

        if (expireAt instanceof Long expireAtLong){
            map.put(EXPIRE_AT, new Date(expireAtLong));
        }

        // otherwise, do nothing and return
    }


    /**
     * Read a document by coll and id. Return null if object not exist
     *
     * @param coll      collection name
     * @param id        id of document
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument readSuppressing404(String coll, String id, String partition) throws Exception {

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        // TODO: RetryUtil.executeWithRetry
        try(var conn = this.dataSource.getConnection()) {
            var record = TableUtil.readRecord(conn, coll, partition, id);

            if(log.isInfoEnabled()) {
                log.info("read Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());
            }

            if(record == null){
                return null;
            }
            return getCosmosDocument(record);
        }

    }

    /**
     * Update existing data. if not exist, throw Not Found Exception.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument update(String coll, Object data, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "update data " + coll + " " + partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var map = JsonUtil.toMap(data);
        var id = getId(map);
        Checker.checkNotBlank(id, "id");
        checkValidId(id);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

        // add timestamp field "_ts"
        addTimestamp(map);

        // add _expireAt if ttl is set
        addExpireAt(map);

        // add etag for optimistic lock if enabled
        addEtag4(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        PostgresRecord record = null;
        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            record = TableUtil.updateRecord(conn, coll, partition, new PostgresRecord(id, map));
        }

        if (log.isInfoEnabled()){
            log.info("updated Document:{}/docs/{}, partition:{}, account:{}", collectionLink, record.id, partition, getAccount());
        }

        return getCosmosDocument(record);
    }


    /**
     * Partial update existing data(Simple version). Input is a map, and the key/value in the map would be patched to the target document in SET mode.
     *
     * <p>
     * see <a href="https://devblogs.microsoft.com/cosmosdb/partial-document-update-ga/">partial update official docs</a>
     * </p>
     * <p>
     * If you want more complex partial update / patch features, please use patch(TODO) method, which supports ADD / SET / REPLACE / DELETE / INCREMENT and etc.
     * </p>
     *
     * @param coll      collection name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception. If not exist, throw Not Found Exception.
     */
    public CosmosDocument updatePartial(String coll, String id, Object data, String partition)
            throws Exception {

        return updatePartial(coll, id, data, partition, new PartialUpdateOption());
    }

    /**
     * Partial update existing data(Simple version). Input is a map, and the key/value in the map would be patched to the target document in SET mode.
     *
     * <p>
     * see <a href="https://www.mongodb.com/docs/drivers/java/sync/current/usage-examples/updateOne/">partial update official docs</a>
     * </p>
     *
     * @param coll      collection name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @param option    partial update option (no effect for mongodb at present. not implemented)
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception. If not exist, throw Not Found Exception.
     */
    public CosmosDocument updatePartial(String coll, String id, Object data, String partition, PartialUpdateOption option)
            throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "upsert data " + coll + " " + partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var map = JsonUtil.toMap(data);
        Checker.checkNotBlank(id, "id");
        checkValidId(id);

        // Remove id from patchData, because it is not needed for a patch action.
        map.remove(TableUtil.ID);

        // Remove partition key from patchData, because it is not needed for a patch action.
        map.remove(Cosmos.getDefaultPartitionKey());

        // add timestamp field "_ts"
        addTimestamp(map);

        // add _expireAt if ttl is set
        addExpireAt(map);

        // add etag for optimistic lock if enabled
        var etag = map.getOrDefault(TableUtil.ETAG, "").toString();
        // add etag for optimistic lock if enabled
        addEtag4(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        PostgresRecord record = null;

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            record = TableUtil.updatePartialRecord(conn, coll, partition, new PostgresRecord(id, map), option, etag);
        }

        if (log.isInfoEnabled()){
            log.info("updatePartial Document:{}/docs/{}, partition:{}, account:{}", collectionLink, record.id, partition, getAccount());
        }
        return getCosmosDocument(record);

    }

    /**
     * Add "_ts" field to data automatically, for compatibility for cosmosdb
     *
     * @param data
     */
    static void addTimestamp(Map<String, Object> data) {
        // format: 1714546148.123
        // we use milli instead of second in order to get a more stable sort when using "sort" : ["_ts", "DESC"]
        data.put("_ts", TimestampUtil.getTimestampInDouble());
    }


    /**
     * Update existing data. Create a new one if not exist. "id" field must be contained in data.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument upsert(String coll, Object data, String partition) throws Exception {
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "upsert data " + coll + " " + partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var map = JsonUtil.toMap(data);
        var id = map.getOrDefault("id", "").toString();
        Checker.checkNotBlank(id, "id");
        checkValidId(id);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

        // add timestamp field "_ts"
        addTimestamp(map);

        // add _expireAt if ttl is set
        addExpireAt(map);

        // add etag for optimistic lock if enabled
        addEtag4(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        PostgresRecord record = null;

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            record = TableUtil.upsertRecord(conn, coll, partition, new PostgresRecord(id, map));
        }

        if (log.isInfoEnabled()){
            log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, record.id, partition, getAccount());
        }
        return getCosmosDocument(record);
    }

    /**
     * Delete a document. Do nothing if object not exist
     *
     * @param coll      collection name
     * @param id        id of document
     * @param partition partition name
     * @return CosmosDatabase instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDatabase delete(String coll, String id, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(partition, "partition");

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);


        try(var conn = this.dataSource.getConnection()){
            // TODO: RetryUtil
            TableUtil.deleteRecord(conn, coll, partition, id);
        }

        log.info("deleted Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

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
     * @param coll      collection name
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */

    public CosmosDocumentList find(String coll, Condition cond, String partition) throws Exception {

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        var ret = _findToIterator(coll, cond, partition).docs;

        if (log.isInfoEnabled()) {
            log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
        }

        return ret;

    }

    /**
     * find data by condition to iterator and return a CosmosDocumentIterator instead of a list.
     * Using this iterator can suppress memory consumption compared to the normal find method, when dealing with large data(size over 1000).
     *
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
     * var userIterator = db.findToIterator("Collection1", cond);
     * while(userIterator.hasNext()){
     *     var user = userIterator.next().toObject(User.class);
    }
     * <p>
     * }
     *
     * @param coll collection name
     * @param cond condition to find
     * @param partition partition name
     * @return CosmosDocumentIterator
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentIterator findToIterator(String coll, Condition cond, String partition) throws Exception {

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        var iterator = _findToIterator(coll, cond, partition);

        if (log.isInfoEnabled()) {
            log.info("findToIterator Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
        }

        return iterator;
    }

    /**
     * A helper method to do findToIterator by condition. find method is also based on this inner method,
     * converting iterator to List.
     *
     * @param coll      collection name
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentIteratorImpl
     * @throws Exception Cosmos client exception
     */
    PostgresDocumentIteratorImpl _findToIterator(String coll, Condition cond, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");

        if (cond == null) {
            cond = new Condition();
        }

        // TODO crossPartition query

        var querySpec = PGConditionUtil.toQuerySpec(coll, cond, partition);

        // TODO: RetryUtil.executeWithRetry

        try(var conn = this.dataSource.getConnection()) {
            var records = TableUtil.findRecords(conn, coll, partition, querySpec);
            var maps = records.stream().map(r -> r.data).toList();
            return new PostgresDocumentIteratorImpl(new CosmosDocumentList(maps));
        }

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
     * @param coll      collection name
     * @param aggregate Aggregate function and groupBys
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentList aggregate(String coll, Aggregate aggregate, String partition) throws Exception {
        return aggregate(coll, aggregate, Condition.filter(), partition);
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
     * @param coll      collection name
     * @param aggregate Aggregate function and groupBys
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentList aggregate(String coll, Aggregate aggregate, Condition cond, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");

        if (cond == null) {
            cond = new Condition();
        }

        var querySpec = PGConditionUtil.toQuerySpec4Aggregate(coll, cond, aggregate, partition);

        // TODO: RetryUtil.executeWithRetry

        try(var conn = this.dataSource.getConnection()) {
            var records = TableUtil.aggregateRecords(conn, coll, partition, querySpec);
            List<Map<String, Object>> maps = records.stream().map(r -> r.data).toList();
            // Process result of aggregate. convert Long value to Integer if possible.
            // Because "itemsCount: 1L" is not acceptable by some users. They prefer "itemsCount: 1" more.
            maps = PGAggregateUtil.convertAggregateResultsToInteger(maps);
            return new CosmosDocumentList(maps);
        }
    }

    /**
     * Count data by condition(ignores offset and limit)
     * <p>
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
     * @param coll      collection name
     * @param cond      condition to find
     * @param partition partition name
     * @return count of documents
     * @throws Exception Cosmos client exception
     */

    public int count(String coll, Condition cond, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        if (cond == null) {
            cond = new Condition();
        }

        var querySpec = PGConditionUtil.toQuerySpecForCount(coll, cond, partition);

        // TODO: RetryUtil.executeWithRetry

        try(var conn = this.dataSource.getConnection()) {
            var count = TableUtil.countRecords(conn, coll, partition, querySpec);
            if(log.isInfoEnabled()) {
                log.info("count Document:{}, cond:{}, collection:{}, partition:{}, account:{}", coll, cond, collectionLink, partition, getAccount());
            }
            return count;
        }
    }

    /**
     * Increment a number field of a document using json path format(e.g. "/count")
     *
     * <p>
     * see json patch format: <a href="http://jsonpatch.com/">json path</a>
     * <br>
     * see details of increment: <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/partial-document-update#supported-operations">supported operations: increment</a>
     * </p>
     *
     * @param coll      collection
     * @param id        item id
     * @param path      json path
     * @param value     amount of increment
     * @param partition partition for item
     * @return result item
     * @throws Exception CosmosException doing increment
     */
    public CosmosDocument increment(String coll, String id, String path, int value, String partition) throws Exception {
        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);
        var patchOperations = PatchOperations.create().increment(path, value);
        var ret = patch(coll, id, patchOperations, partition);
        log.info("increment Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());
        return ret;
    }


    /**
     * Patch data using JSON-Patch format. (max operations is 10)
     *
     * <p>
     * {@code
     * //例：
     * var operations = CosmosPatchOperations.create()
     * // set or replace a new field
     * .set("/contents/sex", "Male");
     * // insert an item at index 1 for a field of array type
     * .add("/skills/1", "TypeScript")
     * var data = service.patch(host, id, operations);
     * }
     *
     * <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/partial-document-update#supported-operations">supported operations</a>
     * </p>
     *
     * @param coll       collection
     * @param id         id of item
     * @param operations operation list of JSON Patch
     * @param partition  partition
     * @return CosmosDocument after patch
     * @throws Exception CosmosException or other
     */
    public CosmosDocument patch(String coll, String id, PatchOperations operations, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(id, "id");
        checkValidId(id);
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(operations, "operations");

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        Preconditions.checkArgument(operations.size() <= PatchOperations.LIMIT, "Size of operations should be less or equal to 10. We got: %d, which exceed the limit 10", operations.size());

        // Set timestamp
        //operations.set("/_ts", TimestampUtil.getTimestampInDouble());

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        PostgresRecord record = null;
        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            record = TableUtil.patchRecord(conn, coll, partition, id, operations);
        }

        if (log.isInfoEnabled()){
            log.info("patched Document:{}/docs/{}, partition:{}, account:{}", collectionLink, record.id, partition, getAccount());
        }

        return getCosmosDocument(record);
    }


    /**
     * Create batch documents in a single transaction.
     * Note: the maximum number of operations is 100.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instances
     * @throws Exception CosmosException
     */
    public List<CosmosDocument> batchCreate(String coll, List<?> data, String partition) throws Exception {

        doCheckBeforeBatch(coll, data, partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var records = new ArrayList<PostgresRecord>();

        for(var datum : data) {
            Map<String, Object> map = JsonUtil.toMap(datum);

            // add partition info
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // set id(for java-cosmos) before insert
            var id = addId4Postgres(map);

            // add timestamp field "_ts"
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt(map);

            // add etag for optimistic lock if enabled
            addEtag4(map);

            records.add(new PostgresRecord(id, map));
        }

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            TableUtil.batchInsertRecords(conn, coll, partition, records);
            if (log.isInfoEnabled()){
                log.info("batch created Document:{}/docs/, records size:{}, partition:{}, account:{}", collectionLink, records.size(), partition, getAccount());
            }
        } catch (SQLException e){
            log.warn("Error when batch creating records from table '{}.{}'. records size:{}. ", coll, partition, records.size(), e);
            throw e;
        }

        return records.stream().map(r -> getCosmosDocument(r)).toList();
    }

    /**
     * Upsert batch documents in a single transaction.
     * Note: the maximum number of operations is 100.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instances
     * @throws Exception CosmosException
     */
    public List<CosmosDocument> batchUpsert(String coll, List<?> data, String partition) throws Exception {
        doCheckBeforeBatch(coll, data, partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var records = new ArrayList<PostgresRecord>();

        for (var datum : data) {
            Map<String, Object> map = JsonUtil.toMap(datum);

            // add partition info
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // set id(for java-cosmos) before upsert
            var id = addId4Postgres(map);

            // add timestamp field "_ts"
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt(map);

            // add etag for optimistic lock if enabled
            addEtag4(map);

            records.add(new PostgresRecord(id, map));
        }

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            TableUtil.batchUpsertRecords(conn, coll, partition, records);
            if (log.isInfoEnabled()) {
                log.info("batch upserted Document:{}/docs/, records size:{}, partition:{}, account:{}", collectionLink, records.size(), partition, getAccount());
            }
        } catch (SQLException e) {
            log.warn("Error when batch upserting records from table '{}.{}'. records size:{}. ", coll, partition, records.size(), e);
            throw e;
        }

        return records.stream().map(r -> getCosmosDocument(r)).toList();
    }

    /**
     * Delete batch documents in a single transaction.
     * Note: the maximum number of operations is 100.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instances (only id)
     * @throws Exception CosmosException
     */
    public List<CosmosDocument> batchDelete(String coll, List<?> data, String partition) throws Exception {
        doCheckBeforeBatch(coll, data, partition);

        var ids = new ArrayList<String>();

        // Extract IDs from data to be deleted
        for (var obj : data) {
            var id = getId(obj);
            checkValidId(id);
            if (StringUtils.isNotEmpty(id)) {
                ids.add(id);
            }
        }

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            TableUtil.batchDeleteRecords(conn, coll, partition, ids);
            if (log.isInfoEnabled()){
                log.info("batch deleted Document:{}/docs/, records size:{}, partition:{}, account:{}", collectionLink, ids.size(), partition, getAccount());
            }
        } catch(SQLException e){
            log.warn("Error when batch deleting records from table '{}.{}'. records size:{}. ", coll, partition, ids.size(), e);
            throw e;
        }

        return ids.stream().map(id -> new CosmosDocument(Map.of("id", id))).toList();
    }


    static void doCheckBeforeBatch(String coll, List<?> data, String partition) {
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotEmpty(data, "create data " + coll + " " + partition);

        checkBatchMaxOperations(data);
        checkValidId(data);
    }

    static void doCheckBeforeBulk(String coll, List<?> data, String partition) {
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotEmpty(data, "create data " + coll + " " + partition);
    }

    /**
     * Bulk create documents.
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosBulkResult
     */
    public CosmosBulkResult bulkCreate(String coll, List<?> data, String partition) throws Exception {

        doCheckBeforeBulk(coll, data, partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var records = new ArrayList<PostgresRecord>();

        for(var datum : data) {
            Map<String, Object> map = JsonUtil.toMap(datum);

            // add partition info
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // set id(for java-cosmos) before insert
            var id = addId4Postgres(map);

            // add timestamp field "_ts"
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt(map);

            // add etag for optimistic lock if enabled
            addEtag4(map);

            records.add(new PostgresRecord(id, map));
        }

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            var ret = TableUtil.bulkInsertRecords(conn, coll, partition, records);
            if (log.isInfoEnabled()){
                log.info("bulk created Document:{}/docs/, records size:{}, partition:{}, account:{}", collectionLink, records.size(), partition, getAccount());
            }
            return ret;
        } catch (SQLException e){
            log.warn("Error when bulk creating records from table '{}.{}'. records size:{}. ", coll, partition, records.size(), e);
            throw e;
        }

    }

    /**
     * Bulk upsert documents
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosBulkResult
     */
    public CosmosBulkResult bulkUpsert(String coll, List<?> data, String partition) throws Exception {

        doCheckBeforeBulk(coll, data, partition);

        coll = TableUtil.checkAndNormalizeValidEntityName(coll);

        var records = new ArrayList<PostgresRecord>();

        for(var datum : data) {
            Map<String, Object> map = JsonUtil.toMap(datum);

            // add partition info
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // set id(for java-cosmos) before upsert
            var id = addId4Postgres(map);

            // add timestamp field "_ts"
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt(map);

            // add etag for optimistic lock if enabled
            addEtag4(map);

            records.add(new PostgresRecord(id, map));
        }

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            var ret = TableUtil.bulkUpsertRecords(conn, coll, partition, records);
            if (log.isInfoEnabled()){
                log.info("bulk upserted Document:{}/docs/, records size:{}, partition:{}, account:{}", collectionLink, records.size(), partition, getAccount());
            }
            return ret;
        } catch (SQLException e){
            log.warn("Error when bulk upserting records from table '{}.{}'. records size:{}. ", coll, partition, records.size(), e);
            throw e;
        }

    }

    /**
     * Bulk delete documents
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosBulkResult
     */
    public CosmosBulkResult bulkDelete(String coll, List<?> data, String partition) throws Exception {

        doCheckBeforeBulk(coll, data, partition);

        var ids = new ArrayList<String>();

        // Extract IDs from data to be deleted
        for (var obj : data) {
            var id = getId(obj);
            checkValidId(id);
            if (StringUtils.isNotEmpty(id)) {
                ids.add(id);
            }
        }

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        try (var conn = this.dataSource.getConnection()) {
            // TODO: RetryUtil.executeWithRetry
            var ret = TableUtil.bulkDeleteRecords(conn, coll, partition, ids);
            if (log.isInfoEnabled()){
                log.info("bulk deleted Document:{}/docs/, deleted count:{}, partition:{}, account:{}", collectionLink, ids.size(), partition, getAccount());
            }
            return ret;
        } catch (SQLException e){
            log.warn("Error when bulk deleting records from table '{}.{}'. records size:{}. ", coll, partition, ids.size(), e);
            throw e;
        }
    }


    static void checkBatchMaxOperations(List<?> data) {
        // There's a current limit of 100 operations per TransactionalBatch to ensure the performance is as expected and within SLAs:
        // https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=dotnet#limitations
        if (data.size() > MAX_BATCH_NUMBER_OF_OPERATION) {
            throw new IllegalArgumentException("The number of data operations should not exceed 100.");
        }
    }


    /**
     * Get cosmos db account id associated with this instance.
     *
     * @return
     * @throws Exception Cosmos client exception
     */
    String getAccount() throws Exception {
        return this.cosmosAccount.getAccount();
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

    /**
     * Enable TTL feature for a given collection and partition.
     *
     * @param coll         collection name
     * @param partition    partition name
     * @return             job name
     * @throws Exception   if the table does not exist or a database error occurs
     */
    public String enableTTL(String coll, String partition) throws Exception {
        // default to 1min
        return enableTTL(coll, partition, 1);
    }

    /**
     * Enable TTL feature for a given collection and partition.
     *
     * @param coll         collection name
     * @param partition    partition name
     * @param intervalInMinutes interval in minutes
     * @return             job name
     * @throws Exception   if the table does not exist or a database error occurs
     */
    public String enableTTL(String coll, String partition, int intervalInMinutes) throws Exception {

        try(var conn = this.dataSource.getConnection()){

            try {
                conn.setAutoCommit(false);

                if (TTLUtil.jobExists(conn, coll, partition)) {
                    return TTLUtil.getJobName(coll, partition);
                }

                TTLUtil.scheduleJob(conn, coll, partition, intervalInMinutes);
                conn.commit();
                return TTLUtil.getJobName(coll, partition);

            } catch (SQLException e){
                log.warn("Error when enableTTL for partition '{}.{}'.", coll, partition, e);
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    log.error("Failed to rollback when enableTTL for partition '{}.{}'. id:{}.", coll, partition, rollbackEx);
                }
                throw e;
            }
            finally {
                conn.setAutoCommit(true);
            }
        }

    }

    /**
     * Disable TTL feature for a given collection and partition.
     *
     * @param coll         collection name
     * @param partition    partition name
     * @return true if the job is successfully disabled, false if the job is not found
     * @throws Exception   if the table does not exist or a database error occurs
     */
    public boolean disableTTL(String coll, String partition) throws Exception {
        try(var conn = this.dataSource.getConnection()){
            return TTLUtil.unScheduleJob(conn, coll, partition);
        }
    }
}
