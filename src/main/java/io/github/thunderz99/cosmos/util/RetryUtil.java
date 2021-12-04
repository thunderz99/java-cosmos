package io.github.thunderz99.cosmos.util;

import java.util.concurrent.Callable;

import com.azure.cosmos.implementation.RequestRateTooLargeException;
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
            try {
                i++;
                return func.call();
            } catch (RequestRateTooLargeException e) {
                if (e.getStatusCode() == 429 || e.getMessage().contains("Request rate is large")) {
                    if (i > maxRetries) {
                        throw e;
                    }
                    var wait = e.getRetryAfterDuration().toMillis();
                    if (wait == 0) {
                        wait = defaultWaitTime;
                    }
                    log.info("429 Too Many Requests. Wait:{} ms", wait);
                    Thread.sleep(wait);
                } else {
                    throw e;
                }
            }
        }
	}

}
