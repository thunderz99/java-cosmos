package io.github.thunderz99.cosmos.util;

import java.util.concurrent.Callable;

import com.microsoft.azure.documentdb.DocumentClientException;
import io.github.thunderz99.cosmos.CosmosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deal with the 429 Reqeust rage is large problem by retry after a certain period
 */
public class RetryUtil {

	private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

	RetryUtil(){
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
            } catch (com.azure.cosmos.CosmosException ce) {
                cosmosException = new CosmosException(ce);
            }

            if (cosmosException.getStatusCode() == 429 || cosmosException.getMessage().contains("Request rate is large")) {
                if (i > maxRetries) {
                    throw cosmosException;
                }
                var wait = cosmosException.getRetryAfterInMilliseconds();
                if (wait == 0) {
                    wait = defaultWaitTime;
                }
                log.info("429 Too Many Requests. Wait:{} ms", wait);
                Thread.sleep(wait);
            } else {
                throw cosmosException;
            }
        }
	}

}
