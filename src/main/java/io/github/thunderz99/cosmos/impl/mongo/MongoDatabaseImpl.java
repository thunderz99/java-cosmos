package io.github.thunderz99.cosmos.impl.mongo;

import java.util.*;
import java.util.stream.Collectors;

import com.azure.cosmos.implementation.guava25.collect.Lists;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.google.common.base.Preconditions;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.*;
import io.github.thunderz99.cosmos.*;
import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.dto.FilterOptions;
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

/**
 * Class representing a database instance.
 *
 * <p>
 * Can do document' CRUD and find.
 * </p>
 */
public class MongoDatabaseImpl implements CosmosDatabase {

    private static Logger log = LoggerFactory.getLogger(MongoDatabaseImpl.class);

    static final int MAX_BATCH_NUMBER_OF_OPERATION = CosmosLimits.BATCH_OPERATION_LIMIT;
    static final int BULK_PATCH_CHUNK_SIZE = CosmosLimits.BULK_CHUNK_SIZE;

    /**
     * field automatically added to contain the expiration timestamp
     */
    public static final String EXPIRE_AT = "_expireAt";

    /**
     * field automatically added to contain the etag value for optimistic lock
     */
    public static final String ETAG = "_etag";

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
        map.put(Cosmos.getDefaultPartitionKey(), partition);

        // set id(for java-cosmos) and _id(for mongo) before insert
        addId4Mongo(map);

        // add timestamp field "_ts"
        addTimestamp(map);

        // add _expireAt if ttl is set
        addExpireAt4Mongo(map);

        // add etag for optimistic lock if enabled
        addEtag4Mongo(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        var container = this.client.getDatabase(coll).getCollection(partition);
        var response = RetryUtil.executeWithRetry(() -> container.insertOne(
                new Document(map)
        ));

        var item = response.getInsertedId();

        log.info("created Document:{}/docs/{}, partition:{}, account:{}", collectionLink, getId(item), partition, getAccount());

        return getCosmosDocument(map);
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

    /**
     * add "_expireAt" field automatically if expireAtEnabled is true, and "ttl" has int value
     *
     * @param objectMap
     * @return expireAt Date. or null if not set.
     */
    Date addExpireAt4Mongo(Map<String, Object> objectMap) {

        var account = (MongoImpl) this.getCosmosAccount();
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
    String addEtag4Mongo(Map<String, Object> objectMap) {

        var account = (MongoImpl) this.getCosmosAccount();
        if (!account.etagEnabled) {
            return null;
        }

        var etag = UUID.randomUUID().toString();
        objectMap.put(ETAG, etag);

        return etag;
    }

    static String getId(Object object) {
        String id;
        if (object instanceof String) {
            id = (String) object;
        } else if (object instanceof BsonObjectId) {
            id = ((BsonObjectId) object).getValue().toHexString();
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
            throw new CosmosException(404, "404", "Resource Not Found. code: NotFound");
        }
        return getCosmosDocument(response);
    }

    /**
     * process precision of timestamp and get CosmosDucment instance from response
     *
     * @param map
     * @return cosmos document
     */
    static CosmosDocument getCosmosDocument(Map<String, Object> map) {
        TimestampUtil.processTimestampPrecision(map);
        return new CosmosDocument(map);
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

        // add _expireAt if ttl is set
        addExpireAt4Mongo(map);

        // add etag for optimistic lock if enabled
        addEtag4Mongo(map);

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

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

        // 1) materialize patch as Map (mutable)
        var patchData = JsonUtil.toMap(data);

        addTimestamp(patchData);
        addExpireAt4Mongo(patchData);

        var incomingEtag = patchData.getOrDefault(ETAG, "").toString();
        addEtag4Mongo(patchData); // generate the NEW etag to be stored after update

        // partition key is not part of the patch
        patchData.remove(Cosmos.getDefaultPartitionKey());

        // 2) decide path: fast $set OR safe replace if any empty keys exist
        boolean hasEmptyKey = MapUtil.containsEmptyKeyDeep(patchData);

        var container = this.client.getDatabase(coll).getCollection(partition);
        Document document = null;

        if (!hasEmptyKey) {
            // ---------- fast path: normal $set with flattened keys ----------
            var flatMap = MapUtil.toFlatMapWithPeriod(patchData);

            if (option.checkETag && StringUtils.isNotEmpty(incomingEtag)) {
                document = RetryUtil.executeWithRetry(() -> container.findOneAndUpdate(
                        com.mongodb.client.model.Filters.and(
                                eq("_id", id),
                                eq(ETAG, incomingEtag)
                        ),
                        new Document("$set", new Document(flatMap)),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER))
                );

                if (document == null) {
                    var confirmDoc = RetryUtil.executeWithRetry(() -> container.find(eq("_id", id)).first());
                    if (confirmDoc != null) {
                        throw new CosmosException(412, "412 Precondition Failed",
                                "failed to updatePartial because etag not match. coll:%s, partition:%s, id:%s, etag:%s"
                                        .formatted(coll, partition, id, incomingEtag));
                    }
                }
            } else {
                document = RetryUtil.executeWithRetry(() -> container.findOneAndUpdate(
                        eq("_id", id),
                        new Document("$set", new Document(flatMap)),
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER))
                );
            }
        } else {
            // ---------- safe path: handle empty keys by read–merge–replace ----------
            // Load current doc
            var current = RetryUtil.executeWithRetry(() -> container.find(eq("_id", id)).first());
            if (current == null) {
                // keep same behavior as before (not found handled later by checkAndGetCosmosDocument)
                return checkAndGetCosmosDocument(null);
            }

            // Merge: existing <- patch (deep for maps), preserving fields we don't touch
            // Make a mutable copy to avoid mutating the original Document instance unexpectedly
            Map<String, Object> merged = new LinkedHashMap<>(current);
            // NEVER allow replacing _id
            merged.put("_id", current.get("_id"));

            merged = MapUtil.merge(merged, patchData);

            var replacement = new Document(merged);

            if (option.checkETag && StringUtils.isNotEmpty(incomingEtag)) {
                document = RetryUtil.executeWithRetry(() -> container.findOneAndReplace(
                        com.mongodb.client.model.Filters.and(
                                eq("_id", id),
                                eq(ETAG, incomingEtag)
                        ),
                        replacement,
                        new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
                );

                if (document == null) {
                    var confirmDoc = RetryUtil.executeWithRetry(() -> container.find(eq("_id", id)).first());
                    if (confirmDoc != null) {
                        // etag mismatch
                        throw new CosmosException(412, "412 Precondition Failed",
                                "failed to updatePartial because etag not match. coll:%s, partition:%s, id:%s, etag:%s"
                                        .formatted(coll, partition, id, incomingEtag));
                    }
                }
            } else {
                document = RetryUtil.executeWithRetry(() -> container.findOneAndReplace(
                        eq("_id", id),
                        replacement,
                        new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER))
                );
            }
        }

        var documentLink = LinkFormatUtil.getDocumentLink(coll, partition, id);
        log.info("updated Document:{}, id:{}, partition:{}, account:{}", documentLink, id, partition, getAccount());

        return checkAndGetCosmosDocument(document);

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

        // add _expireAt if ttl is set
        addExpireAt4Mongo(map);

        // add _expireAt if ttl is set
        addExpireAt4Mongo(map);

        // add etag for optimistic lock if enabled
        addEtag4Mongo(map);

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        // add partition info
        map.put(Cosmos.getDefaultPartitionKey(), partition);

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

        var collectionLink = LinkFormatUtil.getCollectionLink(coll, partition);

        var docs = _findToIterable(coll, cond, partition).into(new ArrayList<>())
                .stream().map(doc -> getCosmosDocument(doc).toMap()).toList();

        var ret = new CosmosDocumentList(docs);

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

        var findIterable = _findToIterable(coll, cond, partition);

        var ret = new MongoDocumentIteratorImpl(findIterable);

        if (log.isInfoEnabled()) {
            log.info("find Document:{}, cond:{}, partition:{}, account:{}", collectionLink, cond, cond.crossPartition ? "crossPartition" : partition, getAccount());
        }

        return ret;
    }

    MongoIterable<Document> _findToIterable(String coll, Condition cond, String partition) throws Exception {

        if (cond == null) {
            cond = new Condition();
        }

        if (CollectionUtils.isNotEmpty(cond.join) && !cond.returnAllSubArray) {
            // When doing join and only return the matching part of subArray,
            // we have to use findWithJoin method, which do a special aggregate pipeline to achieve this
            return _findToIterableWithJoin(coll, cond, partition);
        }

        // TODO crossPartition query

        var filter = ConditionUtil.toBsonFilter(cond);

        // process sort
        var sort = ConditionUtil.toBsonSort(cond.sort);

        var container = this.client.getDatabase(coll).getCollection(partition);

        var findIterable = container.find(filter)
                .sort(sort).skip(cond.offset).limit(cond.limit);

        var fields = ConditionUtil.processFields(cond.fields);
        if (!fields.isEmpty()) {
            // process fields
            findIterable.projection(fields(excludeId(), include(fields)));
        }

        return findIterable;
    }

    /**
     * inner find method. Find documents when JOIN is used and returnAllSubArray is false.
     * In mongo this is implemented by aggregate pipeline and using $project stage and $filter
     *
     * <p>
     * For further information, look at "docs/find-with-join.md"
     * </p>
     *
     * @param coll
     * @param cond
     * @param partition
     * @return aggregateIterable
     */
    AggregateIterable<Document> _findToIterableWithJoin(String coll, Condition cond, String partition) {

        Checker.check(CollectionUtils.isNotEmpty(cond.join), "join cannot be empty in findWithJoin");
        Checker.check(!cond.negative, "Top negative condition is not supported for findWithJoin");
        Checker.check(!cond.returnAllSubArray, "findWithJoin should be used when returnAllSubArray = false");

        var container = this.client.getDatabase(coll).getCollection(partition);

        // Create the aggregation pipeline stages
        var pipeline = new ArrayList<Bson>();

        // 1.1 match stage
        // Add the first $match stage based on filter,  which narrows the pipeline significantly.
        // Process the condition into a BSON filter
        // @see docs/find-with-join.md
        var filter = ConditionUtil.toBsonFilter(cond);
        // Add the match stage based on the filter
        if (filter != null) {
            pipeline.add(Aggregates.match(filter));
        }

        // 1.2 sort stage
        var sort = ConditionUtil.toBsonSort(cond.sort);
        if (sort != null) {
            pipeline.add(Aggregates.sort(sort));
        }

        // 1.3 skip / limit stage
        pipeline.add(Aggregates.skip(cond.offset));
        pipeline.add(Aggregates.limit(cond.limit));

        // 2. project stage
        // Add the $project stage to filter(using $filter) values in arrays specified by cond.join
        // extract the filters related to join in order to get the matching array elements
        var joinRelatedFilters = JoinUtil.extractJoinFilters(cond.filter, cond.join);

        var projectStage = new Document();
        projectStage.append("original", "$$ROOT");

        for (var joinField : cond.join) {
            // Generate a new field for each join-related field with the matching logic
            var matchingFieldName = "matching_" + AggregateUtil.convertFieldNameIncludingDot(joinField);
            var fieldNameInDocument = "$" + joinField;

            // extract the joinFilter starts with the same joinField
            var joinFilter = joinRelatedFilters.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(joinField + "."))
                    .collect(Collectors.toMap(
                            entry -> StringUtils.removeStart(entry.getKey(), joinField + "."), // Remove the joinField prefix
                            Map.Entry::getValue
                    ));

            // Create the BSON expression for the filtered field

            // Construct the filter condition as needed
            var filterConditions = new ArrayList<Bson>();
            joinFilter.forEach((key, value) -> {
                var singleFilter = ConditionUtil.toBsonFilter("$$this." + key, value, FilterOptions.create().join(cond.join).innerCond(true));
                if (singleFilter != null) {
                    filterConditions.add(singleFilter);
                }
            });

            // Assuming only one condition to match, you can use $and or just directly the condition
            projectStage.append(matchingFieldName, new Document("$filter",
                    new Document("input", fieldNameInDocument)
                            .append("cond", filterConditions.size() == 1 ? filterConditions.get(0) : Filters.and(filterConditions))
            ));
        }

        pipeline.add(Aggregates.project(projectStage));

        // 3. replaceRoot stage
        // Merge the original document with the new fields using $replaceRoot and $mergeObjects
        var mergeObjectsFields = new ArrayList<>();
        mergeObjectsFields.add("$original");
        for (var joinField : cond.join) {
            var matchingFieldName = "matching_" + AggregateUtil.convertFieldNameIncludingDot(joinField);
            mergeObjectsFields.add(new Document(matchingFieldName, "$" + matchingFieldName));
        }

        pipeline.add(Aggregates.replaceRoot(new Document("$mergeObjects", mergeObjectsFields)));

        // 4. replaceWith stage
        // Because returnAllSubArray is false, replace the original documents' array with matched elements only
        /*
        // Use $replaceWith to replace nested fields
          {
            $replaceWith: {
              $mergeObjects: [
                "$$ROOT",
                {
                  area: {
                    city: {
                      street: {
                        name: "$area.city.street.name",  // Preserve original street name
                        rooms: "$matching_area__city__street__name"  // Replace rooms with matched rooms
                      }
                    }
                  }
                },
                {
                  "room*no-01": "$matching_room*no-01"  // Replace room*no-01 with matched elements
                }
              ]
            }
          }
         */
        List<Object> mergesTargets = Lists.newArrayList("$$ROOT");

        for (var joinField : cond.join) {
            mergesTargets.add(ConditionUtil.generateMergeObjects(joinField, "matching_" + AggregateUtil.convertFieldNameIncludingDot(joinField)));
        }

        // add a replaceWith stage
        pipeline.add(Aggregates.replaceWith(new Document("$mergeObjects", mergesTargets)));


        // 5 project stage to extract specific fields only
        var fields = ConditionUtil.processFields(cond.fields);
        if (!fields.isEmpty()) {
            // process fields
            pipeline.add(Aggregates.project(Projections.fields(excludeId(), include(fields))));
        }

        // Execute the aggregation pipeline
        return container.aggregate(pipeline);

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
    static List<? extends Map> convertAggregateResultsToInteger(List<? extends Map> maps) {

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
        var filter = ConditionUtil.toBsonFilter(cond);

        // Create the aggregation pipeline stages
        List<Bson> pipeline = new ArrayList<>();

        // 1. Add the match stage based on the filter
        if (filter != null) {
            pipeline.add(Aggregates.match(filter));
        }

        // 2. Add the project stage to rename fields with dots
        var projectStage = AggregateUtil.createProjectStage(aggregate);
        if (!projectStage.toBsonDocument().isEmpty()) {
            pipeline.add(projectStage);
        }

        // 3. Add the group stage
        var groupStage = AggregateUtil.createGroupStage(aggregate);
        if (CollectionUtils.isNotEmpty(groupStage)) {
            pipeline.addAll(groupStage);
        }

        // 4. Add optional offset, limit if specified in Condition
        // We do not need inner offset and limit in aggregate. Please set them in condAfterAggregate

        // 5. Add a final project stage to flatten the _id and rename fields
        var finalProjectStage = AggregateUtil.createFinalProjectStage(aggregate);
        pipeline.add(finalProjectStage);

        // 6. add filter / sort / offset / limit using condAfterAggregate

        var condAfter = aggregate.condAfterAggregate;
        if (condAfter != null) {
            // 6.1 filter
            var filterAfter = ConditionUtil.processNor(ConditionUtil.toBsonFilter(condAfter));
            if (filterAfter != null) {
                pipeline.add(Aggregates.match(filterAfter));
            }

            // 6.2 sort
            var sort = ConditionUtil.toBsonSort(condAfter.sort);
            if (sort != null) {
                pipeline.add(Aggregates.sort(sort));
            }

            // 6.3 offset / limit
            pipeline.add(Aggregates.skip(condAfter.offset));
            pipeline.add(Aggregates.limit(condAfter.limit));
        }

        // Execute the aggregation pipeline
        List<Document> results = container.aggregate(pipeline).into(new ArrayList<>());

        // after process if an aggregate result is empty
        if (results.isEmpty()) {
            results = AggregateUtil.processEmptyAggregateResults(aggregate, results);
        } else {
            // convert aggregate result to Integer if possible
            convertAggregateResultsToInteger(results);
        }

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

        var filter = ConditionUtil.toBsonFilter(cond);

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
        operations.set("/_ts", TimestampUtil.getTimestampInDouble());
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
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // Handle ID
            var id = addId4Mongo(map);

            // Add _ts field
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt4Mongo(map);

            // add etag for optimistic lock if enabled
            addEtag4Mongo(map);


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

                log.info("Batch created Documents in collection:{}, partition:{}, createdCount:{}, account:{}",
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

        return documents.stream().map(doc -> getCosmosDocument(doc)).collect(Collectors.toList());

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
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // Handle ID
            var id = addId4Mongo(map);

            // Add _ts field
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt4Mongo(map);

            // add etag for optimistic lock if enabled
            addEtag4Mongo(map);

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

                log.info("Batch upserted Documents in collection:{}, partition:{}, upsertedCount:{}, account:{}",
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

        return documents.stream().map(doc -> getCosmosDocument(doc)).collect(Collectors.toList());
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
            var id = getId(obj);
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

                log.info("Batch deleted Documents in collection:{}, partition:{}, deletedCount:{}, account:{}",
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

    static void doCheckBeforeBulkPatch(String coll, List<String> ids, PatchOperations operations, String partition) {
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotEmpty(ids, "bulkPatch ids " + coll + " " + partition);
        Checker.checkNotNull(operations, "bulkPatch operations");
        Preconditions.checkArgument(operations.size() <= PatchOperations.LIMIT,
                "Size of operations should be less or equal to 10. We got: %d, which exceed the limit 10",
                operations.size());
        checkValidId(ids);
    }

    static void doCheckBeforeBulkPatch(String coll, List<BulkPatchOperation> data, String partition) {
        Checker.checkNotBlank(coll, "coll");
        Checker.checkNotBlank(partition, "partition");
        Checker.checkNotEmpty(data, "bulkPatch data " + coll + " " + partition);

        for (var operation : data) {
            Checker.checkNotNull(operation, "bulkPatch operation");
            Checker.checkNotBlank(operation.id, "bulkPatch operation id");
            checkValidId(operation.id);
            Checker.checkNotNull(operation.operations, "bulkPatch operation patch operations");
            Preconditions.checkArgument(operation.operations.size() <= PatchOperations.LIMIT,
                    "Size of operations should be less or equal to 10. We got: %d, which exceed the limit 10",
                    operation.operations.size());
        }
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
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // add _id for mongo
            var id = addId4Mongo(map);

            // add _ts field
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt4Mongo(map);

            // add etag for optimistic lock if enabled
            addEtag4Mongo(map);

            var document = new Document(map);
            // prepare documents to insert
            documents.add(document);
            documentsMap.put(id, document);
        }

        var container = this.client.getDatabase(coll).getCollection(partition);

        // do bulk operations
        var result = container.insertMany(documents);

        var inserted = result.getInsertedIds();
        log.info("Bulk created Documents in collection:{}, partition:{}, createdCount:{}, account:{}",
                coll, partition, inserted.size(), getAccount());

        // generate the bulk result
        var ret = new CosmosBulkResult();
        var insertedIdSet = inserted.entrySet().stream().map(k -> k.getValue().asString().getValue()).collect(Collectors.toCollection(LinkedHashSet::new));

        for (var id : documentsMap.keySet()) {
            if (insertedIdSet.contains(id)) {
                ret.successList.add(getCosmosDocument(documentsMap.get(id)));
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
            map.put(Cosmos.getDefaultPartitionKey(), partition);

            // Handle ID
            var id = addId4Mongo(map);

            // Add _ts field
            addTimestamp(map);

            // add _expireAt if ttl is set
            addExpireAt4Mongo(map);

            // add etag for optimistic lock if enabled
            addEtag4Mongo(map);

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

        var totalModifiedCount = bulkWriteResult.getModifiedCount() + bulkWriteResult.getUpserts().size();

        log.info("Bulk upserted Documents in collection:{}, partition:{}, upsertedCount:{}, account:{}",
                coll, partition, totalModifiedCount, getAccount());

        // generate the bulk result
        var ret = new CosmosBulkResult();

        var index = 0;
        for (var entry : documentsMap.entrySet()) {
            if (index < totalModifiedCount) {
                ret.successList.add(getCosmosDocument(entry.getValue()));
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
            var id = getId(obj);
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

    /**
     * Bulk patch documents with the same patch operations.
     *
     * @param coll       collection name
     * @param ids        target document ids
     * @param operations patch operations
     * @param partition  partition name
     * @return CosmosBulkResult
     * @throws Exception cosmos exception
     */
    public CosmosBulkResult bulkPatch(String coll, List<String> ids, PatchOperations operations, String partition) throws Exception {
        doCheckBeforeBulkPatch(coll, ids, operations, partition);

        // Keep the same timestamp for this bulk execution to avoid per-item mutation.
        var operationsWithTs = operations.copy().set("/_ts", TimestampUtil.getTimestampInDouble());
        var patchData = JsonPatchUtil.toMongoPatchData(operationsWithTs);

        var container = this.client.getDatabase(coll).getCollection(partition);
        var update = Updates.combine(patchData);
        var totalModifiedCount = 0L;
        var ret = new CosmosBulkResult();

        for (int from = 0; from < ids.size(); from += BULK_PATCH_CHUNK_SIZE) {
            var to = Math.min(from + BULK_PATCH_CHUNK_SIZE, ids.size());
            var chunkIds = ids.subList(from, to);
            int matchedCount = 0;

            // Intentionally use per-id updateOne here instead of bulkWrite:
            // we need deterministic success/failure classification for each id (including not-found ids),
            // while bulkWrite only exposes aggregated counters and does not map unmatched items by id.
            for (var id : chunkIds) {
                try {
                    var updateResult = container.updateOne(Filters.eq("_id", id), update);
                    if (updateResult.getMatchedCount() > 0) {
                        ret.successList.add(new CosmosDocument(Map.of("id", id)));
                        matchedCount++;
                    } else {
                        ret.fatalList.add(new CosmosException(500, id, "Failed to patch"));
                    }
                } catch (Exception e) {
                    ret.fatalList.add(new CosmosException(500, id, "Failed to patch", e));
                }
            }

            totalModifiedCount += matchedCount;
        }

        log.info("Bulk patched(same operations) Documents in collection:{}, partition:{}, modifiedCount:{}, account:{}",
                coll, partition, totalModifiedCount, getAccount());

        return ret;
    }

    /**
     * Bulk patch documents with different patch operations.
     *
     * @param coll      collection name
     * @param data      bulk patch operations
     * @param partition partition name
     * @return CosmosBulkResult
     * @throws Exception cosmos exception
     */
    public CosmosBulkResult bulkPatch(String coll, List<BulkPatchOperation> data, String partition) throws Exception {
        doCheckBeforeBulkPatch(coll, data, partition);

        var container = this.client.getDatabase(coll).getCollection(partition);
        var totalModifiedCount = 0L;
        var ret = new CosmosBulkResult();

        for (int from = 0; from < data.size(); from += BULK_PATCH_CHUNK_SIZE) {
            var to = Math.min(from + BULK_PATCH_CHUNK_SIZE, data.size());
            var chunkData = data.subList(from, to);
            int matchedCount = 0;

            // Intentionally use per-id updateOne here instead of bulkWrite:
            // we need deterministic success/failure classification for each id (including not-found ids),
            // while bulkWrite only exposes aggregated counters and does not map unmatched items by id.
            for (var operation : chunkData) {
                try {
                    // Keep per-item operation immutable by copying before adding timestamp.
                    var operationsWithTs = operation.operations.copy().set("/_ts", TimestampUtil.getTimestampInDouble());
                    var patchData = JsonPatchUtil.toMongoPatchData(operationsWithTs);
                    var update = Updates.combine(patchData);

                    var updateResult = container.updateOne(Filters.eq("_id", operation.id), update);
                    if (updateResult.getMatchedCount() > 0) {
                        ret.successList.add(new CosmosDocument(Map.of("id", operation.id)));
                        matchedCount++;
                    } else {
                        ret.fatalList.add(new CosmosException(500, operation.id, "Failed to patch"));
                    }
                } catch (Exception e) {
                    ret.fatalList.add(new CosmosException(500, operation.id, "Failed to patch", e));
                }
            }

            totalModifiedCount += matchedCount;
        }

        log.info("Bulk patched Documents in collection:{}, partition:{}, modifiedCount:{}, account:{}",
                coll, partition, totalModifiedCount, getAccount());

        return ret;
    }

    @Override
    public boolean ping(String coll) throws Exception {
        var docs = this.find(coll, Condition.filter().limit(1), "_ping");
        return docs.size() >= 0;
    }

    static void checkBatchMaxOperations(List<?> data) {
        // There's a current limit of 100 operations per TransactionalBatch to ensure the performance is as expected and within SLAs:
        // https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/transactional-batch?tabs=dotnet#limitations
        if (data.size() > MAX_BATCH_NUMBER_OF_OPERATION) {
            throw new IllegalArgumentException("The number of data operations should not exceed %d.".formatted(MAX_BATCH_NUMBER_OF_OPERATION));
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
