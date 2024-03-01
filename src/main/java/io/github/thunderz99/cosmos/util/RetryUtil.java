package io.github.thunderz99.cosmos.util;

import java.util.Set;
import java.util.concurrent.Callable;

import com.azure.cosmos.models.CosmosBatchResponse;
import com.google.common.collect.Sets;
import com.microsoft.azure.documentdb.DocumentClientException;
import io.github.thunderz99.cosmos.CosmosException;
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

    static final int BATCH_MAX_RETRIES = 10;

    RetryUtil() {
    }

    public static CosmosBatchResponse executeBatchWithRetry(Callable<CosmosBatchResponse> func) throws Exception {
        return executeBatchWithRetry(func, 2000);
    }

    public static CosmosBatchResponse executeBatchWithRetry(Callable<CosmosBatchResponse> func, long defaultWaitTime) throws Exception {
        for (int attempt = 0; attempt < BATCH_MAX_RETRIES; attempt++) {
            var response = func.call();

            if (response.isSuccessStatusCode()) {
                return response;
            } else if (shouldRetry(response.getStatusCode())) {
                long delay = response.getRetryAfterDuration().toMillis();
                if (delay <= 0) {
                    delay = defaultWaitTime;
                    if (delay < 0) {
                        log.warn("retryAfterInMilliseconds {} is minus. Will retry by defaultWaitTime(2000ms). error:{}", delay, response.getErrorMessage());
                    }
                }
                try {
                    log.info("Code:{}, 429 Too Many Requests / 449 Retry with / 408 Request Timeout. Wait:{} ms", response.getStatusCode(), delay);
                    Thread.sleep(delay);
                } catch (InterruptedException ignored) {}
            } else {
               throw new CosmosException(response.getStatusCode(), "", response.getErrorMessage());
            }
        }

        throw new CosmosException(0, "", "Max retries count reached");
    }

    public static <T> T executeWithRetry(Callable<T> func) throws Exception {
        //default wait time is 2s
        return executeWithRetry(func, 2000);
    }

    public static <T> T executeWithRetry(Callable<T> func, long defaultWaitTime) throws Exception {
        var maxRetries = 10;
        var i = 0;
        while (true) {
            CosmosException cosmosException = null;
            try {
                i++;
                return func.call();
            } catch (DocumentClientException dce) {
                // deal with document client exception
                cosmosException = new CosmosException(dce);
            } catch (com.azure.cosmos.CosmosException ce) {
                // deal with sdkv4's CosmosException
                cosmosException = new CosmosException(ce);
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

    /**
     * Judge whether should retry action for this cosmos exception. Currently we will retry for 429/408
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
