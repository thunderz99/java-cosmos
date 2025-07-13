package io.github.thunderz99.cosmos.util;

import java.util.*;
import java.util.concurrent.Callable;

import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosItemOperation;
import com.google.common.collect.Sets;
import com.mongodb.MongoException;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosBatchResponseWrapper;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosDatabaseImpl;
import org.apache.commons.lang3.ObjectUtils;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deal with the 429(too many requests) and 449(retry with) error code by retry after a certain period
 *
 * <p>
 * inspired by official documents: <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/sql/bulk-executor-java">Perform bulk operations on Azure Cosmos DB data</a>
 * </p>
 */
public class RetryUtil {

    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    /**
     * Codes should be retried. see <a href="https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb">http-status-codes-for-cosmosdb</a>
     */
    static final Set<Integer> codesShouldRetry = Sets.newHashSet(429, 449, 408);

    /**
     * max retries for a single CRUD execution
     */
    public static final int SINGLE_EXECUTION_MAX_RETRIES = 10;

    /**
     * Wait time before retry in Millis for a single CRUD execution. 2 seconds
     */
    public static final int SINGLE_EXECUTION_DEFAULT_WAIT_TIME = 2000;

    /**
     * Max wait time before retry in Millis for a single CRUD execution. 30 seconds
     */
    public static final int SINGLE_EXECUTION_MAX_WAIT_TIME = 30000;


    /**
     * An instance of LinkedHashMap<String, Object>, used to get the class instance in a convenience way.
     */
    static final Map<String, Object> mapInstance = new LinkedHashMap<>();


    /**
     * max retries for a batch execution.
     *
     * <p>
     * Which is smaller than single. Because the reason a batch failed is more complicated, we should handle the issue more quickly to the caller.
     * </p>
     */
    public static final int BATCH_EXECUTION_MAX_RETRIES = 3;

    /**
     * Wait time before retry in Millis for a batch execution
     *
     * <p>
     * We set 10 seconds for batch retry delay as default. Longer than a single execution.
     * </p>
     */
    public static final int BATCH_EXECUTION_DEFAULT_WAIT_TIME = 10_000;


    RetryUtil() {
    }

    public static <T> T executeWithRetry(Callable<T> func) throws Exception {
        //default wait time is 2s
        return executeWithRetry(func, SINGLE_EXECUTION_DEFAULT_WAIT_TIME);
    }

    public static <T> T executeWithRetry(Callable<T> func, long defaultWaitTime) throws Exception {
        return executeWithRetry(func, defaultWaitTime, SINGLE_EXECUTION_MAX_RETRIES);
    }

    public static <T> T executeWithRetry(Callable<T> func, long defaultWaitTime, int maxRetries) throws Exception {
        var i = 0;
        while (true) {
            CosmosException cosmosException = null;
            try {
                i++;
                return func.call();
            } catch (com.azure.cosmos.CosmosException ce) {
                // deal with sdkv4's CosmosException
                cosmosException = new CosmosException(ce);
            } catch (MongoException me) {
                cosmosException = new CosmosException(me);
            } catch (PSQLException pe) {
                cosmosException = new CosmosException(pe);
            } catch (CosmosException ce) {
                // deal with java-cosmos's CosmosException
                cosmosException = ce;
            }

            if (shouldRetry(cosmosException)) {
                if (i > maxRetries) {
                    log.warn("RetryUtil exceeded max retries. statusCode:{}, code:{}, retryAfter:{} ms, maxRetries:{}, message:{}", cosmosException.getStatusCode(), cosmosException.getCode(), cosmosException.getRetryAfterInMilliseconds(), maxRetries, cosmosException.getMessage());
                    throw cosmosException;
                }
                var wait = cosmosException.getRetryAfterInMilliseconds();
                if (wait <= 0) {
                    wait = calculateWaitTime(defaultWaitTime, i);

                    if (wait < 0) {
                        log.warn("retryAfterInMilliseconds < 0. Will retry by time({} ms)", wait, cosmosException);
                    }
                }
                log.warn("RetryUtil 429 occurred. statusCode:{}, wait:{} ms, message:{}", cosmosException.getStatusCode(), wait, cosmosException.getMessage());
                Thread.sleep(wait);
            } else {
                log.warn("Exception should not retry occurred. statusCode:{}, code:{}, retryAfter:{} ms, message:{}", cosmosException.getStatusCode(), cosmosException.getCode(), cosmosException.getRetryAfterInMilliseconds(), cosmosException.getMessage());
                throw cosmosException;
            }
        }
    }

    /**
     * calculate wait time(ms) for retry by 2^n, n is the time of retry
     *
     * <p>
     * for example, if defaultWaitTime is 2000, and retry 3 times, the wait time will be 2000, 4000, 8000
     * </p>
     *
     * @param defaultWaitTime
     * @param i retry times
     * @return calculated wait time
     */
    static long calculateWaitTime(long defaultWaitTime, int i) {
        if (defaultWaitTime <= 0){
            defaultWaitTime = SINGLE_EXECUTION_DEFAULT_WAIT_TIME;
        }

        var ret = (long) (defaultWaitTime * Math.pow(2, i-1));

        return Math.min(ret, SINGLE_EXECUTION_MAX_WAIT_TIME);

    }


    /**
     * execute batch with retry. used for cosmosdb only
     * @param func
     * @return
     * @throws Exception
     */
    public static CosmosBatchResponseWrapper executeBatchWithRetry(Callable<CosmosBatchResponseWrapper> func) throws Exception {
        return executeBatchWithRetry(func, BATCH_EXECUTION_DEFAULT_WAIT_TIME, BATCH_EXECUTION_MAX_RETRIES);
    }

    /**
     * execute batch with retry. used for cosmosdb only
     * @param func
     * @param defaultWaitTime
     * @param maxRetries
     * @return
     * @throws Exception
     */
    public static CosmosBatchResponseWrapper executeBatchWithRetry(Callable<CosmosBatchResponseWrapper> func, long defaultWaitTime, int maxRetries) throws Exception {
        return executeWithRetry(() -> {
            var response = func.call();
            if (!response.isSuccessStatusCode()) {
                log.warn("executeBatchWithRetry response not success. statusCode:{}, subStatusCode:{}, message:{}", response.getStatusCode(), response.getSubStatusCode(), response.getErrorMessage());
                throw new CosmosException(response.getStatusCode(), String.valueOf(response.getSubStatusCode()),
                        response.getErrorMessage(), response.getRetryAfterDuration().toMillis());
            }
            return response;
        }, defaultWaitTime, maxRetries);
    }

    /**
     * do common bulk operation(create, upsert, delete) with retry. used in cosmosdb only
     *
     * @param coll           collection name
     * @param operations     operations to be executed
     * @param operationFunc  function to execute the operation
     * @return CosmosBulkResult
     * @throws Exception
     */
    public static CosmosBulkResult executeBulkWithRetry(String coll, List<CosmosItemOperation> operations, CosmosDatabaseImpl.BulkOperationable operationFunc) throws Exception {
        return executeBulkWithRetry(coll, operations,operationFunc, 10);
    }

    /**
     * do common bulk operation(create, upsert, delete) with retry. number of maxRetries if a param, which is more testable for unit test
     *
     * @param coll           collection name
     * @param operations     operations to be executed
     * @param operationFunc  function to execute the operation
     * @param maxRetries     max retry times(default to 3)
     * @return CosmosBulkResult
     * @throws Exception
     */
    static CosmosBulkResult executeBulkWithRetry(String coll, List<CosmosItemOperation> operations, CosmosDatabaseImpl.BulkOperationable operationFunc, int maxRetries) throws Exception {
        var bulkResult = new CosmosBulkResult();
        long delay = 0;
        long maxDelay = 16000;

        var successDocuments = new ArrayList<CosmosDocument>();

        for (int attempt = 0; attempt < maxRetries; attempt++) {

            var retryTasks = new ArrayList<CosmosItemOperation>();
            var execResult = operationFunc.execute(operations);

            for (CosmosBulkOperationResponse<?> result : execResult) {
                var operation = result.getOperation();
                var response = result.getResponse();
                if (ObjectUtils.isEmpty(response)) {
                    continue;
                }
                log.info("Document bulk operation: operation type:{}, request charge:{}, coll:{}, partition:{}",
                        operation.getOperationType().name(), response.getRequestCharge(), coll, operation.getPartitionKeyValue().toString());

                if (RetryUtil.shouldRetry(response.getStatusCode())) {
                    delay = Math.max(delay, response.getRetryAfterDuration().toMillis());
                    log.warn("doBulkWithRetry 429 occurred. Code:{}, coll:{}, partition:{}. operationType:{}, Wait:{} ms",
                            response.getStatusCode(), coll, operation.getPartitionKeyValue().toString(), operation.getOperationType(), delay);
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


    /**
     * Judge whether we should retry for this cosmos exception. Currently, we will retry for 429/449/408
     *
     * @param cosmosException cosmosException
     * @return true/false
     */
    public static boolean shouldRetry(CosmosException cosmosException) {
        return codesShouldRetry.contains(cosmosException.getStatusCode()) || cosmosException.getMessage().contains("Request rate is large");
    }

    public static boolean shouldRetry(int statusCode) {
        return codesShouldRetry.contains(statusCode);
    }

}
