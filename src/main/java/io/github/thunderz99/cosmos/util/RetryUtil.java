package io.github.thunderz99.cosmos.util;

import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Sets;
import com.mongodb.MongoException;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosBatchResponseWrapper;
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
    static final int SINGLE_EXECUTION_MAX_RETRIES = 10;

    /**
     * Wait time before retry in Millis for a single CRUD execution
     */
    static final int SINGLE_EXECUTION_DEFAULT_WAIT_TIME = 2000;


    /**
     * max retries for a batch execution.
     *
     * <p>
     * Which is smaller than single. Because the reason a batch failed is more complicated, we should handle the issue more quickly to the caller.
     * </p>
     */
    static final int BATCH_EXECUTION_MAX_RETRIES = 3;

    /**
     * Wait time before retry in Millis for a batch execution
     *
     * <p>
     * We set 10 seconds for batch retry delay as default. Longer than a single execution.
     * </p>
     */
    static final int BATCH_EXECUTION_DEFAULT_WAIT_TIME = 10_000;


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
            } catch (CosmosException ce) {
                // deal with java-cosmos's CosmosException
                cosmosException = ce;
            }

            if (shouldRetry(cosmosException)) {
                if (i > maxRetries) {
                    throw cosmosException;
                }
                var wait = cosmosException.getRetryAfterInMilliseconds();
                if (wait <= 0) {
                    wait = defaultWaitTime;

                    if (wait < 0) {
                        log.warn("retryAfterInMilliseconds {} is minus. Will retry by defaultWaitTime(2000ms)", wait, cosmosException);
                    }
                }
                log.info("Code:{}, 429 Too Many Requests / 449 Retry with / 408 Request Timeout. Wait:{} ms", cosmosException.getStatusCode(), wait);
                Thread.sleep(wait);
            } else {
                throw cosmosException;
            }
        }
    }


    public static CosmosBatchResponseWrapper executeBatchWithRetry(Callable<CosmosBatchResponseWrapper> func) throws Exception {
        return executeBatchWithRetry(func, BATCH_EXECUTION_DEFAULT_WAIT_TIME, BATCH_EXECUTION_MAX_RETRIES);
    }

    public static CosmosBatchResponseWrapper executeBatchWithRetry(Callable<CosmosBatchResponseWrapper> func, long defaultWaitTime, int maxRetries) throws Exception {
        return executeWithRetry(() -> {
            var response = func.call();
            if (!response.isSuccessStatusCode()) {
                throw new CosmosException(response.getStatusCode(), String.valueOf(response.getSubStatusCode()),
                        response.getErrorMessage(), response.getRetryAfterDuration().toMillis());
            }
            return response;
        }, defaultWaitTime, maxRetries);
    }


    /**
     * Judge whether should retry action for this cosmos exception. Currently we will retry for 429/449/408
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
