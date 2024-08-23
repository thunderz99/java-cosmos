package io.github.thunderz99.cosmos.impl.cosmosdb;

import java.util.*;
import java.util.stream.Collectors;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.*;
import com.google.common.base.Preconditions;
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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.thunderz99.cosmos.condition.Condition.getFormattedKey;

/**
 * Class representing a database instance.
 *
 * <p>
 * Can do document' CRUD and find.
 * </p>
 */
public class CosmosDatabaseImpl implements CosmosDatabase {

    private static Logger log = LoggerFactory.getLogger(CosmosDatabaseImpl.class);

    static final int MAX_BATCH_NUMBER_OF_OPERATION = 100;

    String db;
    CosmosClient clientV4;

    Cosmos cosmosAccount;

    public CosmosDatabaseImpl(Cosmos cosmosAccount, String db) {
        this.cosmosAccount = cosmosAccount;
        this.db = db;
        if (cosmosAccount instanceof CosmosImpl) {
            this.clientV4 = ((CosmosImpl) cosmosAccount).getClientV4();
        }

    }

    /**
     * An instance of LinkedHashMap<String, Object>, used to get the class instance in a convenience way.
     */
    static final LinkedHashMap<String, Object> mapInstance = new LinkedHashMap<>();


    /**
     * Create a document
     *
     * @param coll      collection name
     * @param data      data object
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
        objectMap.put(CosmosImpl.getDefaultPartitionKey(), partition);

        var collectionLink = LinkFormatUtil.getCollectionLink(db, coll);

        checkValidId(objectMap);

        var container = this.clientV4.getDatabase(db).getContainer(coll);
        var response = RetryUtil.executeWithRetry(() -> container.createItem(
                objectMap,
                new PartitionKey(partition),
                new CosmosItemRequestOptions()
        ));

        var item = response.getItem();

        log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, getId(item), partition, getAccount());

        return new CosmosDocument(item);
    }

    static String getId(Object object) {
        String id;
        if (object instanceof String) {
            id = (String) object;
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
     * Create a document using default partition
     *
     * @param coll collection name
     * @param data data Object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument create(String coll, Object data) throws Exception {
        return create(coll, data, coll);
    }


    /**
     * @param coll      collection name
     * @param id        id of the document
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    public CosmosDocument read(String coll, String id, String partition) throws Exception {

        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var response = RetryUtil.executeWithRetry(() -> container.readItem(
                id,
                new PartitionKey(partition),
                mapInstance.getClass()
        ));

        log.info("read Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(response.getItem());
    }

    /**
     * Read a document by coll and id
     *
     * @param coll collection name
     * @param id   id of document
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    public CosmosDocument read(String coll, String id) throws Exception {
        return read(coll, id, coll);
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
            if (CosmosImpl.isResourceNotFoundException(e)) {
                return null;
            }
            throw e;
        }
    }

    /**
     * Read a document by coll and id. Return null if object not exist
     *
     * @param coll collection name
     * @param id   id of document
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument readSuppressing404(String coll, String id) throws Exception {

        return readSuppressing404(coll, id, coll);
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

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        // add partition info
        map.put(CosmosImpl.getDefaultPartitionKey(), partition);

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var response = RetryUtil.executeWithRetry(() -> container.replaceItem(
                map, id,
                new PartitionKey(partition),
                new CosmosItemRequestOptions()
        ));


        log.info("updated Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(response.getItem());
    }


    /**
     * Update existing data. if not exist, throw Not Found Exception.
     *
     * @param coll collection name
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument update(String coll, Object data) throws Exception {
        return update(coll, data, coll);
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
     * see <a href="https://devblogs.microsoft.com/cosmosdb/partial-document-update-ga/">partial update official docs</a>
     * </p>
     * <p>
     * If you want more complex partial update / patch features, please use patch method, which supports ADD / SET / REPLACE / DELETE / INCREMENT and etc.
     * </p>
     *
     * @param coll      collection name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @param option    partial update option
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
        patchData.remove(CosmosImpl.getDefaultPartitionKey());

        if (!option.checkETag || StringUtils.isEmpty(MapUtils.getString(patchData, CosmosImpl.ETAG))) {
            // if don't check etag or etag is empty, remove it.
            patchData.remove(CosmosImpl.ETAG);
        }

        return updatePartialByMerge(coll, id, patchData, partition, option);
    }

    /**
     * Update a document with read / merge / upsert method. this will be used when patch operations' size exceed the limit of 10.
     *
     * @param coll      collection name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception. If not exist, throw Not Found Exception.
     */
    CosmosDocument updatePartialByMerge(String coll, String id, Map<String, Object> data, String partition) throws Exception {
        return updatePartialByMerge(coll, id, data, partition, new PartialUpdateOption());
    }

    /**
     * Update a document with read / merge / upsert method. this will be used when patch operations' size exceed the limit of 10.
     *
     * @param coll      collection name
     * @param id        id of document
     * @param data      data object
     * @param partition partition name
     * @param option    partial update option
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception. If not exist, throw Not Found Exception.
     */
    CosmosDocument updatePartialByMerge(String coll, String id, Map<String, Object> data, String partition, PartialUpdateOption option) throws Exception {

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        var map = RetryUtil.executeWithRetry(() -> {
                    // we will not retry if checkETag is true, this will result in an OCC.
                    // if we do not checkETag, we will get the newest etag from DB and retry replaceDocument.
                    var maxRetry = option.checkETag ? 0 : 3;
                    return replaceDocumentWithRefreshingEtag(coll, id, data, maxRetry, partition);
                }
        );

        log.info("updatePartial Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());
        return new CosmosDocument(map);

    }

    /**
     * Helper function. Read original data and do a merge with partial update data, finally return the merged data.
     *
     * @param coll      collection name
     * @param id        id of data
     * @param data      partial update data
     * @param partition partition name
     * @return merged data
     * @throws Exception
     */
    Map<String, Object> readAndMerge(String coll, String id, Map<String, Object> data, String partition) throws Exception {
        var origin = read(coll, id, partition).toMap();

        var newData = JsonUtil.toMap(data);
        // add partition info
        newData.put(CosmosImpl.getDefaultPartitionKey(), partition);

        // this is like `Object.assign(origin, newData)` in JavaScript, but support nested merge.
        var merged = merge(origin, newData);

        checkValidId(merged);
        return merged;
    }

    /**
     * Helper function. Do a partial update which etag check and retry.
     *
     * @param coll      collection name
     * @param id        id of data
     * @param data      partial update data
     * @param maxRetry  max retry count when etag not matches
     * @param partition partition name
     * @return Document
     * @throws Exception
     */
    Map<String, Object> replaceDocumentWithRefreshingEtag(String coll, String id, Map<String, Object> data, int maxRetry, String partition) throws Exception {

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        var retriedCount = 0;

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        while (true) {
            var merged = readAndMerge(coll, id, data, partition);
            var etag = merged.getOrDefault(CosmosImpl.ETAG, "").toString();

            try {
                return container.replaceItem(
                        merged, id,
                        new PartitionKey(partition),
                        new CosmosItemRequestOptions().setIfMatchETag(etag)
                ).getItem();

            } catch (com.azure.cosmos.CosmosException e) {
                if (e.getStatusCode() == 412) {
                    // etag not match, 412 Precondition Failed
                    retriedCount++;
                    if (retriedCount <= maxRetry) {
                        // continue to retry if less than max retries
                        continue;
                    }
                }

                // throw the exception to outer, if code is not 412 or exceeds max retries
                throw e;
            }
        }
    }

    /**
     * Update existing data. Partial update supported(Only 1st json hierarchy supported). If not exist, throw Not Found Exception.
     *
     * <p>
     * see <a href="https://devblogs.microsoft.com/cosmosdb/partial-document-update-ga/">partial update official docs</a>
     * </p>
     *
     * @param coll collection name
     * @param id   id of document
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument updatePartial(String coll, String id, Object data) throws Exception {
        return updatePartial(coll, id, data, coll);
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

        var map = JsonUtil.toMap(data);
        var id = map.getOrDefault("id", "").toString();
        Checker.checkNotBlank(id, "id");
        checkValidId(id);

        var collectionLink = LinkFormatUtil.getCollectionLink(db, coll);

        // add partition info
        map.put(CosmosImpl.getDefaultPartitionKey(), partition);

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var response = RetryUtil.executeWithRetry(() -> container.upsertItem(
                map,
                new PartitionKey(partition),
                new CosmosItemRequestOptions()
        ));

        log.info("upsert Document:{}/docs/{}, partition:{}, account:{}", collectionLink, id, partition, getAccount());

        return new CosmosDocument(response.getItem());
    }

    /**
     * Update existing data. Create a new one if not exist. "id" field must be contained in data.
     *
     * @param coll collection name
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    public CosmosDocument upsert(String coll, Object data) throws Exception {
        return upsert(coll, data, coll);
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
    public CosmosDatabaseImpl delete(String coll, String id, String partition) throws Exception {

        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(id, "id");
        Checker.checkNotBlank(partition, "partition");

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        try {

            var container = this.clientV4.getDatabase(db).getContainer(coll);

            var response = RetryUtil.executeWithRetry(() -> container.deleteItem(
                    id,
                    new PartitionKey(partition),
                    new CosmosItemRequestOptions()
            ));

            log.info("deleted Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        } catch (Exception e) {
            if (CosmosImpl.isResourceNotFoundException(e)) {
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

        var collectionLink = LinkFormatUtil.getCollectionLink(db, coll);

        var queryRequestOptions = new CosmosQueryRequestOptions();

        if (cond.crossPartition) {
            // In v4, do not set the partitionKey to do a cross partition query
        } else {
            queryRequestOptions.setPartitionKey(new PartitionKey(partition));
        }

        var querySpec = aggregate == null ?
                cond.toQuerySpec() : // normal query
                cond.toQuerySpecForAggregate(aggregate); // aggregate query//

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var ret = new CosmosDocumentList();
        if (Objects.isNull(aggregate) && !cond.joinCondText.isEmpty() && !cond.returnAllSubArray) {
            // process query with join
            var jsonObjs = mergeSubArrayToDoc(coll, cond, querySpec, queryRequestOptions);
            ret = new CosmosDocumentList(jsonObjs);
        } else {
            // process query without join
            var docs = RetryUtil.executeWithRetry(() ->
                    container.queryItems(querySpec.toSqlQuerySpecV4(), queryRequestOptions, mapInstance.getClass()));
            List maps = docs.stream().collect(Collectors.toList());

            if (aggregate != null) {
                // Process result of aggregate. convert Long value to Integer if possible.
                // Because "itemsCount: 1L" is not acceptable by some users. They prefer "itemsCount: 1" more.
                maps = convertAggregateResultsToInteger(maps);
            }

            ret = new CosmosDocumentList(maps);
        }


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

        Map<String, String[]> keyMap = new LinkedHashMap<>();

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var queryText = initJoinSelectPart(cond, querySpec, keyMap);
        var pagedDocs = RetryUtil.executeWithRetry(
                () -> container.queryItems(new SqlQuerySpec(queryText, querySpec.getParametersv4()),  // use new querySpec with join
                        requestOptions, mapInstance.getClass()));
        // cast the docs to Map<String, Object> type (want to do this more elegantly in the future)
        var docs = pagedDocs.stream().map(x -> (Map<String, Object>) x).collect(Collectors.toList());
        var result = mergeArrayValueToDoc(docs, keyMap);

        return result.isEmpty() ? docs : result;
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

        var collectionLink = LinkFormatUtil.getCollectionLink(db, coll);

        var queryRequestOptions = new CosmosQueryRequestOptions();

        if (cond.crossPartition) {
            // In v4, do not set the partitionKey to do a cross partition query
        } else {
            queryRequestOptions.setPartitionKey(new PartitionKey(partition));
        }

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var querySpec = cond.toQuerySpecForCount();

        var docs = RetryUtil.executeWithRetry(
                () -> container.queryItems(querySpec.toSqlQuerySpecV4(), queryRequestOptions, mapInstance.getClass())
        );

        List<Map> maps = docs.stream().collect(Collectors.toList());

        if (log.isInfoEnabled()) {
            log.info("count Document:{}, cond:{}, collection:{}, partition:{}, account:{}", coll, cond, collectionLink, partition, getAccount());
        }

        return Integer.parseInt(maps.get(0).getOrDefault("$1", "0").toString());

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

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        Checker.checkNotNull(this.clientV4, String.format("SDK v4 must be enabled to use increment method. docLink:%s", documentLink));

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var response = RetryUtil.executeWithRetry(() -> container.patchItem(
                id,
                new PartitionKey(partition),
                CosmosPatchOperations
                        .create()
                        .increment(path, value),
                LinkedHashMap.class
        ));

        var item = response.getItem();
        log.info("increment Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(item);
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

        var documentLink = LinkFormatUtil.getDocumentLink(db, coll, id);

        Checker.checkNotNull(this.clientV4, String.format("SDK v4 must be enabled to use patch method. docLink:%s", documentLink));
        Checker.checkNotEmpty("id", "id");
        Checker.checkNotNull(operations, "operations");

        Preconditions.checkArgument(operations.size() <= PatchOperations.LIMIT, "Size of operations should be less or equal to 10. We got: %d, which exceed the limit 10", operations.size());

        var container = this.clientV4.getDatabase(db).getContainer(coll);

        var response = RetryUtil.executeWithRetry(() -> container.patchItem(
                id,
                new PartitionKey(partition),
                operations.getCosmosPatchOperations(),
                LinkedHashMap.class
        ));

        var item = response.getItem();
        log.info("patch Document:{}, partition:{}, account:{}", documentLink, partition, getAccount());

        return new CosmosDocument(item);
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

        var partitionKey = new PartitionKey(partition);
        var container = this.clientV4.getDatabase(db).getContainer(coll);
        CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);
        data.forEach(it -> {
            var map = JsonUtil.toMap(it);
            map.put(CosmosImpl.getDefaultPartitionKey(), partition);
            batch.createItemOperation(map);
        });

        return doBatchWithRetry(container, batch);
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

        var partitionKey = new PartitionKey(partition);
        var container = this.clientV4.getDatabase(db).getContainer(coll);
        CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);
        data.forEach(it -> {
            var map = JsonUtil.toMap(it);
            map.put(CosmosImpl.getDefaultPartitionKey(), partition);
            batch.upsertItemOperation(map);
        });

        return doBatchWithRetry(container, batch);
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

        var partitionKey = new PartitionKey(partition);
        var container = this.clientV4.getDatabase(db).getContainer(coll);
        CosmosBatch batch = CosmosBatch.createCosmosBatch(partitionKey);

        var ids = new ArrayList<String>();
        data.stream().map(CosmosDatabaseImpl::getId).filter(ObjectUtils::isNotEmpty).forEach(it -> {
            ids.add(it);
            batch.deleteItemOperation(it);
        });

        doBatchWithRetry(container, batch);

        return ids.stream().map(it ->
                new CosmosDocument(Map.of("id", it))
        ).collect(Collectors.toList());
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
    public CosmosBulkResult bulkCreate(String coll, List<?> data, String partition) throws Exception {
        doCheckBeforeBulk(coll, data, partition);

        var partitionKey = new PartitionKey(partition);
        var operations = data.stream().map(it -> {
                    var map = JsonUtil.toMap(it);
                    map.put(CosmosImpl.getDefaultPartitionKey(), partition);
                    return CosmosBulkOperations.getCreateItemOperation(map, partitionKey);
                }
        ).collect(Collectors.toList());

        return doBulkWithRetry(coll, operations);
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

        var partitionKey = new PartitionKey(partition);
        var operations = data.stream().map(it -> {
                    var map = JsonUtil.toMap(it);
                    map.put(CosmosImpl.getDefaultPartitionKey(), partition);
                    return CosmosBulkOperations.getUpsertItemOperation(map, partitionKey);
                }
        ).collect(Collectors.toList());

        return doBulkWithRetry(coll, operations);
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
        var partitionKey = new PartitionKey(partition);
        var operations = data.stream()
                .map(it -> {
                    var id = getId(it);
                    ids.add(id);
                    return id;
                })
                .filter(ObjectUtils::isNotEmpty)
                .map(it -> CosmosBulkOperations.getDeleteItemOperation(it, partitionKey))
                .collect(Collectors.toList());

        var result = doBulkWithRetry(coll, operations);

        result.successList = ids.stream().map(it ->
                new CosmosDocument(Map.of("id", it))
        ).collect(Collectors.toList());


        return result;
    }

    private CosmosBulkResult doBulkWithRetry(String coll, List<CosmosItemOperation> operations) {
        var container = this.clientV4.getDatabase(db).getContainer(coll);
        var bulkResult = new CosmosBulkResult();

        int maxRetries = 10;
        long delay = 0;
        long maxDelay = 16000;

        var successDocuments = new ArrayList<CosmosDocument>();

        for (int attempt = 0; attempt < maxRetries; attempt++) {

            var retryTasks = new ArrayList<CosmosItemOperation>();
            var execResult = container.executeBulkOperations(operations);

            for (CosmosBulkOperationResponse<?> result : execResult) {
                var operation = result.getOperation();
                var response = result.getResponse();
                if (ObjectUtils.isEmpty(response)) {
                    continue;
                }

                if (RetryUtil.shouldRetry(response.getStatusCode())) {
                    delay = Math.max(delay, response.getRetryAfterDuration().toMillis());
                    retryTasks.add(operation);
                } else if (response.isSuccessStatusCode()) {
                    var item = response.getItem(mapInstance.getClass());
                    if (item == null) continue;
                    successDocuments.add(new CosmosDocument(item));
                } else {
                    var ex = result.getException();
                    if (HttpConstants.StatusCodes.CONFLICT == response.getStatusCode()) {
                        Map<String, String> map = operation.getItem();
                        bulkResult.fatalList.add(new CosmosException(response.getStatusCode(), "CONFLICT", "id already exits: " + map.get("id")));
                    } else {
                        if (ObjectUtils.isNotEmpty(ex)) {
                            bulkResult.fatalList.add(new CosmosException(response.getStatusCode(), ex.getMessage(), ex.getMessage()));
                        } else {
                            bulkResult.fatalList.add(new CosmosException(response.getStatusCode(), "UNKNOWN", "UNKNOWN"));
                        }
                    }
                }
            }

            if (retryTasks.isEmpty()) {
                operations.clear();
                break;
            } else {
                operations = retryTasks;
            }

            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {
            }
            // Exponential Backoff
            delay = Math.min(maxDelay, delay * 2);
        }

        bulkResult.retryList = operations;
        bulkResult.successList = successDocuments;
        return bulkResult;
    }

    static void checkBatchMaxOperations(List<?> data) {
        // There's a current limit of 100 operations per TransactionalBatch to ensure the performance is as expected and within SLAs:
        // https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=dotnet#limitations
        if (data.size() > MAX_BATCH_NUMBER_OF_OPERATION) {
            throw new IllegalArgumentException("The number of data operations should not exceed 100.");
        }
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
