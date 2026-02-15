package io.github.thunderz99.cosmos;

import java.util.List;

import io.github.thunderz99.cosmos.condition.Aggregate;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.dto.BulkPatchOperation;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.v4.PatchOperations;

/**
 * Class representing a nosql database interface.
 *
 * <p>
 * Can do document's CRUD and find. Supports cosmosdb and mongodb
 * </p>
 */
public interface CosmosDatabase {


    /**
     * Create a document
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Cosmos Client Exception
     */
    public CosmosDocument create(String coll, Object data, String partition) throws Exception;


    /**
     * Create a document using default partition
     *
     * @param coll collection name
     * @param data data Object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    default public CosmosDocument create(String coll, Object data) throws Exception {
        return create(coll, data, coll);
    }

    /**
     * @param coll      collection name
     * @param id        id of the document
     * @param partition partition name
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    public CosmosDocument read(String coll, String id, String partition) throws Exception;

    /**
     * Read a document by coll and id
     *
     * @param coll collection name
     * @param id   id of document
     * @return CosmosDocument instance
     * @throws Exception Throw 404 Not Found Exception if object not exist
     */
    default public CosmosDocument read(String coll, String id) throws Exception {
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
    public CosmosDocument readSuppressing404(String coll, String id, String partition) throws Exception;

    /**
     * Read a document by coll and id. Return null if object not exist
     *
     * @param coll collection name
     * @param id   id of document
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    default public CosmosDocument readSuppressing404(String coll, String id) throws Exception {
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
    public CosmosDocument update(String coll, Object data, String partition) throws Exception;


    /**
     * Update existing data. if not exist, throw Not Found Exception.
     *
     * @param coll collection name
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    default public CosmosDocument update(String coll, Object data) throws Exception {
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
    default public CosmosDocument updatePartial(String coll, String id, Object data, String partition)
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
     * If you want more complex partial update / patch features, please use patch(TODO) method, which supports ADD / SET / REPLACE / DELETE / INCREMENT and etc.
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
            throws Exception;



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
    default public CosmosDocument updatePartial(String coll, String id, Object data) throws Exception {
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
    public CosmosDocument upsert(String coll, Object data, String partition) throws Exception;

    /**
     * Update existing data. Create a new one if not exist. "id" field must be contained in data.
     *
     * @param coll collection name
     * @param data data object
     * @return CosmosDocument instance
     * @throws Exception Cosmos client exception
     */
    default public CosmosDocument upsert(String coll, Object data) throws Exception {
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
    public CosmosDatabase delete(String coll, String id, String partition) throws Exception;


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

    public CosmosDocumentList find(String coll, Condition cond, String partition) throws Exception;




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

    default public CosmosDocumentList find(String coll, Condition cond) throws Exception {
        return find(coll, cond, coll);
    }

    /**
     * find data by condition to iterator and return a CosmosDocumentIterator instead of a list.
     * Using this iterator can suppress memory consumption compared to the normal find method, when dealing with large data(size over 1000).
     *
     * <p>
     * {@code
     *   var cond = Condition.filter(
     *     "id>=", "id010", // id greater or equal to 'id010'
     *     "lastName", "Banks" // last name equal to Banks
     *   )
     *   .order("lastName", "ASC") //optional order
     *   .offset(0) //optional offset
     *   .limit(100); //optional limit
     *
     *   var userIterator = db.findToIterator("Collection1", cond);
     *   while(userIterator.hasNext()){
     *     var user = userIterator.next().toObject(User.class);
     *   }
     * }
     * </p>
     *
     * @param coll collection name
     * @param cond condition to find
     * @param partition partition name
     * @return CosmosDocumentIterator
     * @throws Exception Cosmos client exception
     */
    public CosmosDocumentIterator findToIterator(String coll, Condition cond, String partition) throws Exception;


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
    public CosmosDocumentList aggregate(String coll, Aggregate aggregate, Condition cond, String partition) throws Exception;

    /**
     * do an aggregate query by Aggregate and empty condition
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
    default public CosmosDocumentList aggregate(String coll, Aggregate aggregate, String partition) throws Exception {
        return aggregate(coll, aggregate, Condition.filter(), partition);
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
    default public CosmosDocumentList aggregate(String coll, Aggregate aggregate, Condition cond) throws Exception {
        return aggregate(coll, aggregate, cond, coll);
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

    public int count(String coll, Condition cond, String partition) throws Exception;

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
    public CosmosDocument increment(String coll, String id, String path, int value, String partition) throws Exception;


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
    public CosmosDocument patch(String coll, String id, PatchOperations operations, String partition) throws Exception;


    /**
     * Get cosmos db account instance associated with this instance.
     *
     * @return cosmosAccount
     */
    public Cosmos getCosmosAccount();

    /**
     * Get cosmos database name associated with this instance.
     *
     * @return database name
     */
    public String getDatabaseName();

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
    public List<CosmosDocument> batchCreate(String coll, List<?> data, String partition) throws Exception;

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
    public List<CosmosDocument> batchUpsert(String coll, List<?> data, String partition) throws Exception;

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
    public List<CosmosDocument> batchDelete(String coll, List<?> data, String partition) throws Exception;


    /**
     * Bulk create documents.
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosBulkResult
     */
    public CosmosBulkResult bulkCreate(String coll, List<?> data, String partition) throws Exception;

    /**
     * Bulk upsert documents
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosBulkResult
     */
    public CosmosBulkResult bulkUpsert(String coll, List<?> data, String partition) throws Exception;

    /**
     * Bulk delete documents
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * @param coll      collection name
     * @param data      data object
     * @param partition partition name
     * @return CosmosBulkResult
     */
    public CosmosBulkResult bulkDelete(String coll, List<?> data, String partition) throws Exception;

    /**
     * Bulk patch documents with the same patch operations.
     *
     * <p>
     * Default implementation converts ids to BulkPatchOperation list and delegates to
     * {@link #bulkPatch(String, List, String)}.
     * </p>
     *
     * @param coll       collection name
     * @param ids        target document ids
     * @param operations patch operations applied to each target id
     * @param partition  partition name
     * @return CosmosBulkResult
     * @throws Exception cosmos exception
     */
    default CosmosBulkResult bulkPatch(String coll, List<String> ids, PatchOperations operations, String partition) throws Exception {
        if (ids == null) {
            return this.bulkPatch(coll, (List<BulkPatchOperation>) null, partition);
        }

        var data = ids.stream()
                .map(id -> BulkPatchOperation.of(id, operations == null ? null : operations.copy()))
                .toList();
        return this.bulkPatch(coll, data, partition);
    }

    /**
     * Bulk patch documents.
     * Note: Non-transaction. Have no number limit in theoretically.
     *
     * <p>
     * Default implementation executes one-by-one patch for compatibility.
     * </p>
     *
     * @param coll      collection name
     * @param data      bulk patch operations (id + PatchOperations)
     * @param partition partition name
     * @return CosmosBulkResult
     */
    default CosmosBulkResult bulkPatch(String coll, List<BulkPatchOperation> data, String partition) throws Exception {
        var ret = new CosmosBulkResult();
        if (data == null) {
            return ret;
        }

        for (var operation : data) {
            try {
                if (operation == null) {
                    throw new CosmosException(400, "BAD_REQUEST", "bulkPatch operation should not be null");
                }
                ret.successList.add(this.patch(coll, operation.id, operation.operations, partition));
            } catch (CosmosException ce) {
                ret.fatalList.add(ce);
            } catch (Exception e) {
                ret.fatalList.add(new CosmosException(500, "UNKNOWN", e.getMessage(), e));
            }
        }

        return ret;
    }

    /**
     * Ping a collection to test whether it is accessible.
     *
     * @param coll      collection name
     * @return true/false
     */
    public boolean ping(String coll) throws Exception;


}
