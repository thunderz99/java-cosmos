package io.github.thunderz99.cosmos.impl.mongo;

import java.util.*;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.CosmosBatchOperationResult;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Updates;
import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosDatabase;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosDocumentList;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.dto.CosmosBatchResponseWrapper;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.util.*;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.eq;
import static io.github.thunderz99.cosmos.condition.Condition.getFormattedKey;

/**
 * Class representing a database instance.
 *
 * <p>
 * Can do document' CRUD and find.
 * </p>
 */
public class MongoDatabaseImpl implements CosmosDatabase {

    private static Logger log = LoggerFactory.getLogger(MongoDatabaseImpl.class);

    static final int MAX_BATCH_NUMBER_OF_OPERATION = 100;

    String db;
    MongoClient client;

    Cosmos cosmosAccount;

    public MongoDatabaseImpl(Cosmos cosmosAccount, String db) {
        this.cosmosAccount = cosmosAccount;
        this.db = db;
        if (cosmosAccount instanceof MongoImpl) {
            this.client = ((MongoImpl) cosmosAccount).getClient();
        }

    }

    /**
     * An instance of LinkedHashMap<String, Object>, used to get the class instance in a convenience way.
     */
    static final LinkedHashMap<String, Object> mapInstance = new LinkedHashMap<>();


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


        Map<String, Object> objectMap = JsonUtil.toMap(data);

        // add partition info
        objectMap.put(MongoImpl.getDefaultPartitionKey(), partition);

        // set id(for java-cosmos) and _id(for mongo) before insert
        processId4Mongo(objectMap);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        checkValidId(objectMap);

        var container = this.client.getDatabase(coll).getCollection(partition);
        var response = RetryUtil.executeWithRetry(() -> container.insertOne(
                convert(objectMap)
        ));

        var item = response.getInsertedId();

        log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, getId(item), partition, getAccount());

        return new CosmosDocument(objectMap);
    }

    /**
     * set _id correctly by "id" for mongodb
     * @param objectMap
     */
    static void processId4Mongo(Map<String, Object> objectMap) {
        var id = objectMap.getOrDefault("id", UUID.randomUUID()).toString();
        objectMap.put("id", id);
        objectMap.put("_id", id);
    }



    static String getId(Object object) {
        String id;
        if (object instanceof String) {
            id = (String) object;
        } else if(object instanceof BsonObjectId) {
            id = ((BsonObjectId) object).getValue().toHexString();
        } else {
            var map = JsonUtil.toMap(object);
            id = map.getOrDefault("id", "").toString();
        }
        return id;
    }


    static void checkValidId(List<?> data) {
        for (Object datum : data) {
            if (datum instanceof String) {
                checkValidId((String) datum);
            } else {
                Map<String, Object> map = JsonUtil.toMap(datum);
                checkValidId(map);
            }
        }
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
        var id = getId(objectMap);
        checkValidId(id);
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

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var response = RetryUtil.executeWithRetry(() -> container.find(eq("_id", id)).first()
        );

        log.info("read Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(response);
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

        try {
            return read(coll, id, partition);
        } catch (Exception e) {
            if (MongoImpl.isResourceNotFoundException(e)) {
                return null;
            }
            throw e;
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

        var map = JsonUtil.toMap(data);
        var id = getId(map);

        Checker.checkNotBlank(id, "id");
        checkValidId(id);

        // process id for mongo
        map.put("_id", id);

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        // add partition info
        map.put(MongoImpl.getDefaultPartitionKey(), partition);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var response = RetryUtil.executeWithRetry(() -> container.replaceOne(eq("_id", id), convert(map))
        );

        log.info("updated Document:{}, id:{}, partition:{}, account:{}", documentLink, id, partition, getAccount());

        return new CosmosDocument(map);
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

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "updatePartial data " + coll + " " + partition);

        checkValidId(id);

        var patchData = JsonUtil.toMap(data);

        // Remove partition key from patchData, because it is not needed for a patch action.
        patchData.remove(MongoImpl.getDefaultPartitionKey());

        Updates.combine();

        //TODO etag

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var response = RetryUtil.executeWithRetry(() -> container.updateOne(eq("_id", id), convert(patchData))
        );


        log.info("updated Document:{}, id:{}, partition:{}, account:{}", documentLink, response.getUpsertedId().toString(), partition, getAccount());

        return read(coll, id, partition);


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

        var map = JsonUtil.toMap(data);
        var id = map.getOrDefault("id", "").toString();

        Checker.checkNotBlank(id, "id");
        checkValidId(id);
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotNull(data, "upsert data " + coll + " " + partition);

        // process id for mongo
        map.put("_id", id);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        // add partition info
        map.put(MongoImpl.getDefaultPartitionKey(), partition);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var response = RetryUtil.executeWithRetry(() -> container.replaceOne(eq("_id", id), convert(map), new ReplaceOptions().upsert(true))
        );

        log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

        return new CosmosDocument(map);
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

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var response = RetryUtil.executeWithRetry(() -> container.deleteOne(eq("_id", id))
        );

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
        // do a find without aggregate
        return find(coll, null, cond, partition);

    }

    /**
     * A helper method to do find/aggregate by condition
     *
     * @param coll      collection name
     * @param aggregate aggregate settings. null if no aggregation needed.
     * @param cond      condition to find
     * @param partition partition name
     * @return CosmosDocumentList
     * @throws Exception Cosmos client exception
     */
    CosmosDocumentList find(String coll, Aggregate aggregate, Condition cond, String partition) throws Exception {

        throw new NotImplementedException();
//        var collectionLink = CosmosImpl.getCollectionLink(db, coll);
//
//        var queryRequestOptions = new CosmosQueryRequestOptions();
//
//        if (cond.crossPartition) {
//            // In v4, do not set the partitionKey to do a cross partition query
//        } else {
//            queryRequestOptions.setPartitionKey(new PartitionKey(partition));
//        }
//
//        var querySpec = cond.toQuerySpec(aggregate);
//
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//
//        var ret = new CosmosDocumentList();
//        if (Objects.isNull(aggregate) && !cond.joinCondText.isEmpty() && !cond.returnAllSubArray) {
//            // process query with join
//            var jsonObjs = mergeSubArrayToDoc(coll, cond, querySpec, queryRequestOptions);
//            ret = new CosmosDocumentList(jsonObjs);
//        } else {
//            // process query without join
//            var docs = RetryUtil.executeWithRetry(() ->
//                    container.queryItems(querySpec.toSqlQuerySpecV4(), queryRequestOptions, mapInstance.getClass()));
//            List maps = docs.stream().collect(Collectors.toList());
//
//            if (aggregate != null) {
//                // Process result of aggregate. convert Long value to Integer if possible.
//                // Because "itemsCount: 1L" is not acceptable by some users. They prefer "itemsCount: 1" more.
//                maps = convertAggregateResultsToInteger(maps);
//            }
//
//            ret = new CosmosDocumentList(maps);
//        }
//
//
//        if (log.isInfoEnabled()) {
//            log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
//        }
//
//        return ret;

    }

    /**
     * Process result of aggregate. convert Long value to Integer if possible.
     * <p>
     * Because "itemsCount: 1L" is not acceptable by some users. They prefer "itemsCount: 1" more.
     * </p>
     *
     * @param maps
     * @return
     */
    static List<? extends LinkedHashMap> convertAggregateResultsToInteger(List<? extends LinkedHashMap> maps) {

        if (CollectionUtils.isEmpty(maps)) {
            return maps;
        }

        for (var map : maps) {
            map.replaceAll((key, value) -> {
                // Check if the value is an instance of Long
                if (value instanceof Number) {
                    var numberValue = (Number) value;
                    return NumberUtil.convertNumberToIntIfCompatible(numberValue);
                }
                return value; // Return the original value if no conversion is needed
            });
        }

        return maps;
    }

    /**
     * Merge the sub array to origin array
     * This function will traverse the result of join part and replaced by new result that is found by sub query.
     *
     * @param coll           collection name
     * @param cond           merge the content of the sub array to origin array
     * @param querySpec      querySpec
     * @param requestOptions request options
     * @return docs list
     * @throws Exception error exception
     */
    List<Map<String, Object>> mergeSubArrayToDoc(String coll, Condition cond, CosmosSqlQuerySpec querySpec, CosmosQueryRequestOptions requestOptions) throws Exception {

        throw new NotImplementedException();
//        Map<String, String[]> keyMap = new LinkedHashMap<>();
//
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//
//        var queryText = initJoinSelectPart(cond, querySpec, keyMap);
//        var pagedDocs = RetryUtil.executeWithRetry(
//                () -> container.queryItems(new SqlQuerySpec(queryText, querySpec.getParametersv4()),  // use new querySpec with join
//                        requestOptions, mapInstance.getClass()));
//        // cast the docs to Map<String, Object> type (want to do this more elegantly in the future)
//        var docs = pagedDocs.stream().map(x -> (Map<String, Object>) x).collect(Collectors.toList());
//        var result = mergeArrayValueToDoc(docs, keyMap);
//
//        return result.isEmpty() ? docs : result;
    }

    /**
     * This function will traverse the result of join part and replaced by new result that is found by sub query.
     *
     * @param docs   docs
     * @param keyMap join part map
     * @return the merged sub array
     */
    List<Map<String, Object>> mergeArrayValueToDoc(List<Map<String, Object>> docs, Map<String, String[]> keyMap) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (var doc : docs) {
            var docMain = JsonUtil.toMap(doc.get("c"));

            for (Map.Entry<String, String[]> entry : keyMap.entrySet()) {
                if (Objects.nonNull(doc.get(entry.getKey()))) {
                    Map<String, Object> docSubListItem = Map.of(entry.getKey(), doc.get(entry.getKey()));
                    traverseListValueToDoc(docMain, docSubListItem, entry, 0);
                }
            }
            result.add(docMain);
        }

        return result;
    }

    /**
     * Init the select part of join
     *
     * @param cond      condition
     * @param querySpec query spec
     * @param keyMap    join part map
     * @return select part
     */
    private String initJoinSelectPart(Condition cond, CosmosSqlQuerySpec querySpec, Map<String, String[]> keyMap) {
        StringBuilder queryText = new StringBuilder();

        var originSelectPart = querySpec.getQueryText().substring(0, querySpec.getQueryText().indexOf("WHERE"));
        queryText.append(String.format("SELECT DISTINCT (%s) c", originSelectPart));

        int count = 0;

        for (Map.Entry<String, List<String>> condText : cond.joinCondText.entrySet()) {
            var condList = condText.getValue();
            var joinPart = condText.getKey();
            var aliasName = "s" + count++;
            var condStr = condList.stream().map(item -> item.replace(getFormattedKey(joinPart), "s")).collect(Collectors.joining(" AND "));

            keyMap.put(aliasName, condText.getKey().split("\\."));
            queryText.append(String.format(", ARRAY(SELECT VALUE s FROM s IN %s WHERE %s) %s", getFormattedKey(condText.getKey()), condStr, aliasName));
        }

        int startIndex = querySpec.getQueryText().indexOf("FROM c");
        queryText.append(querySpec.getQueryText().substring(startIndex - 1));

        return queryText.toString();
    }

    /**
     * Traverse and merge the content of the list to origin list
     * This function will traverse the result of join part and replaced by new result that is found by sub query.
     *
     * @param docMap    the map of doc
     * @param newSubMap new sub map
     * @param entry     entry
     * @param count     count
     */
    void traverseListValueToDoc(Map<String, Object> docMap, Map<String, Object> newSubMap, Map.Entry<String, String[]> entry, int count) {

        var aliasName = entry.getKey();
        var subValue = entry.getValue();

        if (count == subValue.length - 1) {
            if (newSubMap.get(aliasName) instanceof List) {
                docMap.put(subValue[entry.getValue().length - 1], newSubMap.get(aliasName));
            }
            return;
        }

        if (docMap.get(subValue[count]) instanceof Map) {
            traverseListValueToDoc((Map) docMap.get(subValue[count++]), newSubMap, entry, count);
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
        throw new NotImplementedException();
//
//        var collectionLink = CosmosImpl.getCollectionLink(db, coll);
//
//        var queryRequestOptions = new CosmosQueryRequestOptions();
//
//        if (cond.crossPartition) {
//            // In v4, do not set the partitionKey to do a cross partition query
//        } else {
//            queryRequestOptions.setPartitionKey(new PartitionKey(partition));
//        }
//
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//
//        var querySpec = cond.toQuerySpecForCount();
//
//        var docs = RetryUtil.executeWithRetry(
//                () -> container.queryItems(querySpec.toSqlQuerySpecV4(), queryRequestOptions, mapInstance.getClass())
//        );
//
//        List<Map> maps = docs.stream().collect(Collectors.toList());
//
//        if (log.isInfoEnabled()) {
//            log.info("count Document:{}, cond:{}, collection:{}, partition:{}, account:{}", coll, cond, collectionLink, partition, getAccount());
//        }
//
//        return Integer.parseInt(maps.get(0).getOrDefault("$1", "0").toString());

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
        throw new NotImplementedException();
//
//        var documentLink = CosmosImpl.getDocumentLink(db, coll, id);
//
//        Checker.checkNotNull(this.clientV4, String.format("SDK v4 must be enabled to use increment method. docLink:%s", documentLink));
//
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//
//        var response = RetryUtil.executeWithRetry(() -> container.patchItem(
//                id,
//                new PartitionKey(partition),
//                CosmosPatchOperations
//                        .create()
//                        .increment(path, value),
//                LinkedHashMap.class
//        ));
//
//        var item = response.getItem();
//        log.info("increment Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());
//
//        return new CosmosDocument(item);
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
        throw new NotImplementedException();
//
//        var documentLink = CosmosImpl.getDocumentLink(db, coll, id);
//
//        Checker.checkNotNull(this.clientV4, String.format("SDK v4 must be enabled to use patch method. docLink:%s", documentLink));
//        Checker.checkNotEmpty("id", "id");
//        Checker.checkNotNull(operations, "operations");
//
//        Preconditions.checkArgument(operations.size() <= PatchOperations.LIMIT, "Size of operations should be less or equal to 10. We got: %d, which exceed the limit 10", operations.size());
//
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//
//        var response = RetryUtil.executeWithRetry(() -> container.patchItem(
//                id,
//                new PartitionKey(partition),
//                operations.getCosmosPatchOperations(),
//                LinkedHashMap.class
//        ));
//
//        var item = response.getItem();
//        log.info("patch Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());
//
//        return new CosmosDocument(item);
    }
    /**
     * like Object.assign(m1, m2) in javascript, but support nested merge.
     *
     * @param m1
     * @param m2
     * @return map after merge
     */
    static Map<String, Object> merge(Map<String, Object> m1, Map<String, Object> m2) {

        for (var entry : m1.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();

            var value2 = m2.get(key);

            // do nested merge
            if (value != null && value instanceof Map<?, ?> && value2 != null && value2 instanceof Map<?, ?>) {
                var subMap1 = (Map<String, Object>) value;
                var subMap2 = (Map<String, Object>) value2;

                subMap1 = merge(subMap1, subMap2);
                m2.put(key, subMap1);
            }

        }


        m1.putAll(m2);
        return m1;
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

        throw new NotImplementedException();
//        doCheckBeforeBatch(coll, data, partition);
//
//        var partitionKey = new PartitionKey(partition);
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//        CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);
//        data.forEach(it -> {
//            var map = JsonUtil.toMap(it);
//            map.put(MongoImpl.getDefaultPartitionKey(), partition);
//            batch.createItemOperation(map);
//        });
//
//        return doBatchWithRetry(container, batch);
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
        throw new NotImplementedException();
//        doCheckBeforeBatch(coll, data, partition);
//
//        var partitionKey = new PartitionKey(partition);
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//        CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);
//        data.forEach(it -> {
//            var map = JsonUtil.toMap(it);
//            map.put(MongoImpl.getDefaultPartitionKey(), partition);
//            batch.upsertItemOperation(map);
//        });
//
//        return doBatchWithRetry(container, batch);
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
        throw new NotImplementedException();
//        doCheckBeforeBatch(coll, data, partition);
//
//        var partitionKey = new PartitionKey(partition);
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//        CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);
//
//        var ids = new ArrayList<String>();
//        data.stream().map(MongoDatabaseImpl::getId).filter(ObjectUtils::isNotEmpty).forEach(it -> {
//            ids.add(it);
//            batch.deleteItemOperation(it);
//        });
//
//        doBatchWithRetry(container, batch);
//
//        return ids.stream().map(it ->
//                new CosmosDocument(Map.of("id", it))
//        ).collect(Collectors.toList());
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

        checkValidId(data);
    }

    private List<CosmosDocument> doBatchWithRetry(CosmosContainer container, CosmosBatch batch) throws Exception {
        var response = RetryUtil.executeBatchWithRetry(() ->
                new CosmosBatchResponseWrapper(container.executeCosmosBatch(batch))
        );

        var successDocuments = new ArrayList<CosmosDocument>();
        for (CosmosBatchOperationResult cosmosBatchOperationResult : response.cosmosBatchReponse.getResults()) {
            var item = cosmosBatchOperationResult.getItem(mapInstance.getClass());
            if (item == null) continue;
            successDocuments.add(new CosmosDocument(item));
        }

        return successDocuments;
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
    public CosmosBulkResult bulkCreate(String coll, List<?> data, String partition) {

        throw new NotImplementedException();
//        doCheckBeforeBulk(coll, data, partition);
//
//        var partitionKey = new PartitionKey(partition);
//        var operations = data.stream().map(it -> {
//                    var map = JsonUtil.toMap(it);
//            map.put(MongoImpl.getDefaultPartitionKey(), partition);
//                    return CosmosBulkOperations.getCreateItemOperation(map, partitionKey);
//                }
//        ).collect(Collectors.toList());
//
//        return doBulkWithRetry(coll, operations);
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
    public CosmosBulkResult bulkUpsert(String coll, List<?> data, String partition) {
        throw new NotImplementedException();
//        doCheckBeforeBulk(coll, data, partition);
//
//        var partitionKey = new PartitionKey(partition);
//        var operations = data.stream().map(it -> {
//                    var map = JsonUtil.toMap(it);
//            map.put(MongoImpl.getDefaultPartitionKey(), partition);
//                    return CosmosBulkOperations.getUpsertItemOperation(map, partitionKey);
//                }
//        ).collect(Collectors.toList());
//
//        return doBulkWithRetry(coll, operations);
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
    public CosmosBulkResult bulkDelete(String coll, List<?> data, String partition) {
        throw new NotImplementedException();
//        doCheckBeforeBulk(coll, data, partition);
//
//        var ids = new ArrayList<String>();
//        var partitionKey = new PartitionKey(partition);
//        var operations = data.stream()
//                .map(it -> {
//                    var id = getId(it);
//                    ids.add(id);
//                    return id;
//                })
//                .filter(ObjectUtils::isNotEmpty)
//                .map(it -> CosmosBulkOperations.getDeleteItemOperation(it, partitionKey))
//                .collect(Collectors.toList());
//
//        var result = doBulkWithRetry(coll, operations);
//
//        result.successList = ids.stream().map(it ->
//                new CosmosDocument(Map.of("id", it))
//        ).collect(Collectors.toList());
//
//
//        return result;
    }

    private CosmosBulkResult doBulkWithRetry(String coll, List<CosmosItemOperation> operations) {
        throw new NotImplementedException();
//        var container = this.clientV4.getDatabase(db).getContainer(coll);
//        var bulkResult = new CosmosBulkResult();
//
//        int maxRetries = 10;
//        long delay = 0;
//        long maxDelay = 16000;
//
//        var successDocuments = new ArrayList<CosmosDocument>();
//
//        for (int attempt = 0; attempt < maxRetries; attempt++) {
//
//            var retryTasks = new ArrayList<CosmosItemOperation>();
//            var execResult = container.executeBulkOperations(operations);
//
//            for (CosmosBulkOperationResponse<?> result : execResult) {
//                var operation = result.getOperation();
//                var response = result.getResponse();
//                if (ObjectUtils.isEmpty(response)) {
//                    continue;
//                }
//
//                if (RetryUtil.shouldRetry(response.getStatusCode())) {
//                    delay = Math.max(delay, response.getRetryAfterDuration().toMillis());
//                    retryTasks.add(operation);
//                } else if (response.isSuccessStatusCode()) {
//                    var item = response.getItem(mapInstance.getClass());
//                    if (item == null) continue;
//                    successDocuments.add(new CosmosDocument(item));
//                } else {
//                    var ex = result.getException();
//                    if (HttpConstants.StatusCodes.CONFLICT == response.getStatusCode()) {
//                        Map<String, String> map = operation.getItem();
//                        bulkResult.fatalList.add(new CosmosException(response.getStatusCode(), "CONFLICT", "id already exits: " + map.get("id")));
//                    } else {
//                        if (ObjectUtils.isNotEmpty(ex)) {
//                            bulkResult.fatalList.add(new CosmosException(response.getStatusCode(), ex.getMessage(), ex.getMessage()));
//                        } else {
//                            bulkResult.fatalList.add(new CosmosException(response.getStatusCode(), "UNKNOWN", "UNKNOWN"));
//                        }
//                    }
//                }
//            }
//
//            if (retryTasks.isEmpty()) {
//                operations.clear();
//                break;
//            } else {
//                operations = retryTasks;
//            }
//
//            try {
//                Thread.sleep(delay);
//            } catch (InterruptedException ignored) {}
//            // Exponential Backoff
//            delay = Math.min(maxDelay, delay * 2);
//        }
//
//        bulkResult.retryList = operations;
//        bulkResult.successList = successDocuments;
//        return bulkResult;
    }

    static void checkBatchMaxOperations(List<?> data) {
        // There's a current limit of 100 operations per TransactionalBatch to ensure the performance is as expected and within SLAs:
        // https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=dotnet#limitations
        if (data.size() > MAX_BATCH_NUMBER_OF_OPERATION) {
            throw new IllegalArgumentException("The number of data operations should not exceed 100.");
        }
    }

    /**
     * convert map obj to bson Document (used in mongodb)
     *
     * @param map
     * @return
     */
    static Document convert(Map<String, Object> map) {
        return new Document(map);
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

}
