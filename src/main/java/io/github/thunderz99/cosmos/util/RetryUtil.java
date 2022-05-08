package io.github.thunderz99.cosmos.util;

import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.collect.Sets;
import com.microsoft.azure.documentdb.DocumentClientException;
import io.github.thunderz99.cosmos.CosmosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deal with the 429(too many requests) and 408(request timeout) error code by retry after a certain period
 *
 * <p>
 * inspired by official documents: <a href="https://docs.microsoft.com/en-us/azure/cosmos-db/sql/bulk-executor-java">Perform bulk operations on Azure Cosmos DB data</a>
 * </p>
 */
public class RetryUtil {

    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    /**
     * Codes should be retries. see <a href="https://docs.microsoft.com/en-us/rest/api/cosmos-db/http-status-codes-for-cosmosdb">http-status-codes-for-cosmosdb</a>
     */
    static final Set<Integer> codesShouldRetry = Sets.newHashSet(429, 449);

    RetryUtil() {
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
                cosmosException = new CosmosException(dce);
            }

            if (shouldRetry(cosmosException)) {
                if (i > maxRetries) {
                    throw cosmosException;
                }
                var wait = cosmosException.getRetryAfterInMilliseconds();
                if (wait == 0) {
                    wait = defaultWaitTime;
                }
                log.info("Code:{}, 429 Too Many Requests / 449 Retry with. Wait:{} ms", cosmosException.getStatusCode(), wait);
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

}
