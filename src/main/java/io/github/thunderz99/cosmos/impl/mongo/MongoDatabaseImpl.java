package io.github.thunderz99.cosmos.impl.mongo;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosBatch;
import com.azure.cosmos.models.CosmosBatchOperationResult;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.google.common.base.Preconditions;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.*;
import io.github.thunderz99.cosmos.*;
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
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
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

        Map<String, Object> map = JsonUtil.toMap(data);

        // add partition info
        map.put(MongoImpl.getDefaultPartitionKey(), partition);

        // set id(for java-cosmos) and _id(for mongo) before insert
        addId4Mongo(map);

        // add timestamp field "_ts"
        addTimestamp(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        checkValidId(map);

        var container = this.client.getDatabase(coll).getCollection(partition);
        var response = RetryUtil.executeWithRetry(() -> container.insertOne(
                new Document(map)
        ));

        var item = response.getInsertedId();

        log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, getId(item), partition, getAccount());

        return new CosmosDocument(map);
    }

    /**
     * set _id correctly by "id" for mongodb
     *
     * @param objectMap
     */
    static String addId4Mongo(Map<String, Object> objectMap) {
        var id = objectMap.getOrDefault("id", UUID.randomUUID()).toString();
        checkValidId(id);
        objectMap.put("id", id);
        objectMap.put("_id", id);
        return id;
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

        return checkAndGetCosmosDocument(response);
    }

    /**
     * check whether the response is null and return CosmosDocument. if null, throw CosmosException(404 Not Found)
     *
     * @param response
     * @return cosmos document
     */
    static CosmosDocument checkAndGetCosmosDocument(Document response) {
        if (response == null) {
            throw new CosmosException(404, "404", "Resource Not Found");
        }
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

        // add timestamp field "_ts"
        addTimestamp(map);

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        // add partition info
        map.put(MongoImpl.getDefaultPartitionKey(), partition);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var document = RetryUtil.executeWithRetry(() -> container.findOneAndReplace(eq("_id", id),
                new Document(map),
                new FindOneAndReplaceOptions().upsert(false).returnDocument(ReturnDocument.AFTER))
        );

        log.info("updated Document:{}, id:{}, partition:{}, account:{}", documentLink, id, partition, getAccount());

        return checkAndGetCosmosDocument(document);
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

        addTimestamp(patchData);

        // Remove partition key from patchData, because it is not needed for a patch action.
        patchData.remove(MongoImpl.getDefaultPartitionKey());

        // flatten the map to "address.country.street" format to be used in mongo update method.
        var flatMap = MapUtil.toFlatMapWithPeriod(patchData);

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var document = RetryUtil.executeWithRetry(() -> container.findOneAndUpdate(eq("_id", id),
                new Document("$set", new Document(flatMap)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER))
        );

        log.info("updated Document:{}, id:{}, partition:{}, account:{}", documentLink, id, partition, getAccount());

        return checkAndGetCosmosDocument(document);

    }

    /**
     * Add "_ts" field to data automatically, for compatibility for cosmosdb
     *
     * @param patchData
     */
    static void addTimestamp(Map<String, Object> patchData) {
        patchData.put("_ts", Instant.now().getEpochSecond());
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

        // add timestamp field "_ts"
        addTimestamp(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        // add partition info
        map.put(MongoImpl.getDefaultPartitionKey(), partition);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var document = RetryUtil.executeWithRetry(() -> container.findOneAndReplace(eq("_id", id),
                new Document(map),
                new FindOneAndReplaceOptions().upsert(true).returnDocument(ReturnDocument.AFTER))
        );

        log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

        return checkAndGetCosmosDocument(document);
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
        var collectionLink = LinkFormatUtil.getCollectionLink(db, coll);

        // TODO crossPartition query

        var filterBeforeProcess = ConditionUtil.toBsonFilter(cond);

        // process top $not filter to $nor for mongo
        var filter = ConditionUtil.processNor(filterBeforeProcess);

        // process sort
        var sort = ConditionUtil.toBsonSort(cond.sort);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var ret = new CosmosDocumentList();

        var findIterable = container.find(filter)
                .sort(sort).skip(cond.offset).limit(cond.limit);

        var fields = ConditionUtil.processFields(cond.fields);
        if (!fields.isEmpty()) {
            // process fields
            findIterable.projection(fields(excludeId(), include(fields)));
        }

        var docs = RetryUtil.executeWithRetry(() -> findIterable.into(new ArrayList<>()));


        ret = new CosmosDocumentList(docs);

        if (log.isInfoEnabled()) {
            log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
        }

        return ret;

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
        var container = this.client.getDatabase(coll).getCollection(partition);

        // Process the condition into a BSON filter
        var filter = ConditionUtil.processNor(ConditionUtil.toBsonFilter(cond));

        // Create the aggregation pipeline stages
        List<Bson> pipeline = new ArrayList<>();

        // 1. Add the match stage based on the filter
        if (filter != null) {
            pipeline.add(Aggregates.match(filter));
        }

        // 2. Add the project stage to rename fields with dots
        var projectStage = AggregateUtil.createProjectStage(aggregate);
        pipeline.add(projectStage);

        // 3. Add the group stage
        var groupStage = AggregateUtil.createGroupStage(aggregate);
        if (groupStage != null) {
            pipeline.add(groupStage);
        }

        // 4. Add optional offset, limit if specified in Condition
        // We do not need offset and limit in aggregate

        // 5. Add a final project stage to flatten the _id and rename fields
        var finalProjectStage = AggregateUtil.createFinalProjectStage(aggregate);
        pipeline.add(finalProjectStage);

        // Execute the aggregation pipeline
        var results = container.aggregate(pipeline).into(new ArrayList<>());

        // Return the results as CosmosDocumentList
        return new CosmosDocumentList(results);
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

        var collectionLink = LinkFormatUtil.getCollectionLink(db, coll);

        // TODO crossPartition query

        var filterBeforeProcess = ConditionUtil.toBsonFilter(cond);

        // process top $not filter to $nor for mongo
        var filter = ConditionUtil.processNor(filterBeforeProcess);

        var container = this.client.getDatabase(coll).getCollection(partition);


        var ret = RetryUtil.executeWithRetry(() -> container.countDocuments(filter));

        if (log.isInfoEnabled()) {
            log.info("count:{}, Document:{}, cond:{}, partition:{}, account:{}", ret, collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
        }

        return Math.toIntExact(ret);
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

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        Checker.checkNotEmpty("id", "id");
        Checker.checkNotNull(operations, "operations");

        Preconditions.checkArgument(operations.size() <= PatchOperations.LIMIT, "Size of operations should be less or equal to 10. We got: %d, which exceed the limit 10", operations.size());

        // Set timestamp
        operations.set("/_ts", Instant.now().getEpochSecond());
        var patchData = JsonPatchUtil.toMongoPatchData(operations);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var document = RetryUtil.executeWithRetry(() -> container.findOneAndUpdate(eq("_id", id),
                Updates.combine(patchData),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER))
        );

        log.info("patched Document:{}, id:{}, partition:{}, account:{}", documentLink, id, partition, getAccount());
        return checkAndGetCosmosDocument(document);
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

        doCheckBeforeBatch(coll, data, partition);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var documents = new ArrayList<Document>();

        // Prepare documents for insertion
        for (Object obj : data) {
            var map = JsonUtil.toMap(obj);

            // Add partition info
            map.put(MongoImpl.getDefaultPartitionKey(), partition);

            // Handle ID
            var id = addId4Mongo(map);

            // Add _ts field
            addTimestamp(map);

            documents.add(new Document(map));
        }

        // Start a client session
        try (var session = this.client.startSession()) {
            // Start a transaction
            session.startTransaction();

            try {
                // Perform batch insertion within the transaction
                container.insertMany(session, documents);

                // Commit the transaction
                session.commitTransaction();

                log.info("Batch created Documents in collection:{}, partition:{}, insertedCount:{}, account:{}",
                        coll, partition, documents.size(), getAccount());

            } catch (Exception e) {
                // Abort the transaction if something goes wrong
                session.abortTransaction();

                if (e instanceof MongoException) {
                    throw new CosmosException((MongoException) e);
                }
                throw new CosmosException(500, "500", "batchCreate Transaction failed: " + e.getMessage(), e);
            }
        }

        return documents.stream().map(doc -> new CosmosDocument(doc)).collect(Collectors.toList());

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

        var container = this.client.getDatabase(coll).getCollection(partition);

        var bulkOperations = new ArrayList<WriteModel<Document>>();

        var documents = new ArrayList<Document>();

        for (Object obj : data) {
            Map<String, Object> map = JsonUtil.toMap(obj);

            // Add partition info
            map.put(MongoImpl.getDefaultPartitionKey(), partition);

            // Handle ID
            var id = addId4Mongo(map);

            // Add _ts field
            addTimestamp(map);

            var document = new Document(map);

            // Create upsert operation
            var filter = Filters.eq("_id", id);
            var update = new Document("$set", document);
            var updateOneModel = new UpdateOneModel<Document>(filter, update, new UpdateOptions().upsert(true));

            bulkOperations.add(updateOneModel);
            documents.add(document);
        }

        // Start a client session
        try (var session = this.client.startSession()) {
            // Start a transaction
            session.startTransaction();

            try {
                // Perform batch insertion within the transaction
                container.bulkWrite(session, bulkOperations);

                // Commit the transaction
                session.commitTransaction();

                log.info("Batch created Documents in collection:{}, partition:{}, insertedCount:{}, account:{}",
                        coll, partition, documents.size(), getAccount());

            } catch (Exception e) {
                // Abort the transaction if something goes wrong
                session.abortTransaction();

                if (e instanceof MongoException) {
                    throw new CosmosException((MongoException) e);
                }
                throw new CosmosException(500, "500", "batchUpsert Transaction failed: " + e.getMessage(), e);
            }
        }

        return documents.stream().map(doc -> new CosmosDocument(doc)).collect(Collectors.toList());
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

        var container = this.client.getDatabase(coll).getCollection(partition);

        var ids = new ArrayList<String>();

        // Extract IDs from data to be deleted
        for (var obj : data) {
            var map = JsonUtil.toMap(obj);
            var id = map.getOrDefault("id", "").toString();
            checkValidId(id);
            if (StringUtils.isNotEmpty(id)) {
                ids.add(id);
            }
        }

        // Start a client session
        try (var session = this.client.startSession()) {
            // Start a transaction
            session.startTransaction();

            try {
                // Perform batch insertion within the transaction
                var result = container.deleteMany(session, Filters.in("_id", ids));

                // Commit the transaction
                session.commitTransaction();

                log.info("Batch created Documents in collection:{}, partition:{}, insertedCount:{}, account:{}",
                        coll, partition, ids.size(), getAccount());


            } catch (Exception e) {
                // Abort the transaction if something goes wrong
                session.abortTransaction();

                if (e instanceof MongoException) {
                    throw new CosmosException((MongoException) e);
                }
                throw new CosmosException(500, "500", "batchDelete Transaction failed: " + e.getMessage(), e);
            }
        }

        return ids.stream().map(id -> new CosmosDocument(Map.of("id", id))).collect(Collectors.toList());
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
    public CosmosBulkResult bulkCreate(String coll, List<?> data, String partition) throws Exception {

        doCheckBeforeBulk(coll, data, partition);

        var documents = new ArrayList<Document>();

        // id -> document map in order to generate the return value
        var documentsMap = new LinkedHashMap<String, Document>();

        for (Object obj : data) {
            Map<String, Object> map = JsonUtil.toMap(obj);

            // add partition info
            map.put(MongoImpl.getDefaultPartitionKey(), partition);

            // add _id for mongo
            var id = addId4Mongo(map);

            // add _ts field
            addTimestamp(map);

            var document = new Document(map);
            // prepare documents to insert
            documents.add(document);
            documentsMap.put(id, document);
        }

        var container = this.client.getDatabase(coll).getCollection(partition);

        // do bulk operations
        var result = container.insertMany(documents);

        var inserted = result.getInsertedIds();
        log.info("Bulk created Documents in collection:{}, partition:{}, insertedCount:{}, account:{}",
                coll, partition, inserted.size(), getAccount());

        // generate the bulk result
        var ret = new CosmosBulkResult();
        var insertedIdSet = inserted.entrySet().stream().map(k -> k.getValue().asString().getValue()).collect(Collectors.toCollection(LinkedHashSet::new));

        for (var id : documentsMap.keySet()) {
            if (insertedIdSet.contains(id)) {
                ret.successList.add(new CosmosDocument(documentsMap.get(id)));
            } else {
                ret.fatalList.add(new CosmosException(500, id, "Failed to insert"));
            }
        }

        return ret;

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

        var container = this.client.getDatabase(coll).getCollection(partition);

        var bulkOperations = new ArrayList<WriteModel<Document>>();

        // id -> document map in order to generate the return value
        var documentsMap = new LinkedHashMap<String, Document>();

        for (Object obj : data) {
            Map<String, Object> map = JsonUtil.toMap(obj);

            // Add partition info
            map.put(MongoImpl.getDefaultPartitionKey(), partition);

            // Handle ID
            var id = addId4Mongo(map);

            // Add _ts field
            addTimestamp(map);

            var document = new Document(map);

            // Create upsert operation
            var filter = Filters.eq("_id", id);
            var update = new Document("$set", document);
            var updateOneModel = new UpdateOneModel<Document>(filter, update, new UpdateOptions().upsert(true));

            bulkOperations.add(updateOneModel);
            documentsMap.put(id, document);
        }

        // Execute bulkWrite operation
        var bulkWriteResult = container.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(true));

        log.info("Bulk created Documents in collection:{}, partition:{}, insertedCount:{}, account:{}",
                coll, partition, bulkWriteResult.getModifiedCount(), getAccount());


        // generate the bulk result
        var ret = new CosmosBulkResult();

        var modifiedCount = bulkWriteResult.getModifiedCount();
        var index = 0;
        for (var entry : documentsMap.entrySet()) {
            if (index < modifiedCount) {
                ret.successList.add(new CosmosDocument(entry.getValue()));
            } else {
                ret.fatalList.add(new CosmosException(500, entry.getKey(), "Failed to upsert"));
            }
            index++;
        }

        return ret;

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
            var map = JsonUtil.toMap(obj);
            var id = map.getOrDefault("id", "").toString();
            checkValidId(id);
            if (StringUtils.isNotEmpty(id)) {
                ids.add(id);
            }
        }

        var container = this.client.getDatabase(coll).getCollection(partition);

        // Perform deleteMany operation using the list of IDs
        var filter = Filters.in("_id", ids);
        var deleteResult = container.deleteMany(filter);

        log.info("Bulk deleted Documents in collection:{}, partition:{}, deletedCount:{}, account:{}",
                coll, partition, deleteResult.getDeletedCount(), getAccount());

        // Generate the bulk result
        var ret = new CosmosBulkResult();

        if (deleteResult.getDeletedCount() > 0) {
            ret.successList = ids.stream()
                    .map(it -> new CosmosDocument(Map.of("id", it)))
                    .collect(Collectors.toList());
        } else {
            for (String id : ids) {
                ret.fatalList.add(new CosmosException(500, id, "Failed to delete"));
            }
        }

        return ret;
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

}