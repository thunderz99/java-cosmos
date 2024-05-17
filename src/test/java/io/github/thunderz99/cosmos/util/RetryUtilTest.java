package io.github.thunderz99.cosmos.util;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.azure.cosmos.implementation.CosmosError;
import io.github.thunderz99.cosmos.CosmosException;
import org.junit.jupiter.api.Test;

import static com.azure.cosmos.implementation.HttpConstants.HttpHeaders.RETRY_AFTER_IN_MILLISECONDS;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeout;

class RetryUtilTest {


    @Test
    void executeWithRetry_should_not_retry_404() throws Exception {

        { // should not retry 404
            assertThatThrownBy(() ->
                    RetryUtil.executeWithRetry(() -> {
                        throw new com.azure.cosmos.CosmosException(404, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "0")) {
                        };
                    })
            ).isInstanceOf(CosmosException.class);
        }
    }

    @Test
    void executeWithRetry_should_succeed_with_max_retries() throws Exception {

        { // success
            var ret = RetryUtil.executeWithRetry(() -> {
                return "OK";
            });
            assertThat(ret).isEqualTo("OK");
        }

        { // success after 2 retries. for 429
            final var i = new AtomicInteger(0);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new com.azure.cosmos.CosmosException(429, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "10")) {
                            };
                        }
                        return "OK";
                    })
            );
            assertThat(ret).isEqualTo("OK");
        }

        { // success after 2 retries. for 449 and com.azure.CosmosException
            final var i = new AtomicInteger(0);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new com.azure.cosmos.CosmosException(449, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "10")) {
                            };
                        }
                        return "OK";
                    })
            );
            assertThat(ret).isEqualTo("OK");
        }

        { // success after 1 retries. for 408 and CosmosException
            final var i = new AtomicInteger(0);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new CosmosException(408, "REQUEST_TIMEOUT", "Request Timeout", 5);
                        }
                        return "OK";
                    })
            );
            assertThat(ret).isEqualTo("OK");
        }
    }

    @Test
    void executeWithRetry_should_throw_exception_after_max_retries() throws Exception {


        // throw exception after 10 retries
        final var i = new AtomicInteger(0);

        assertThatThrownBy(() ->
                RetryUtil.executeWithRetry(() -> {
                    if (i.incrementAndGet() < 12) {
                        throw new com.azure.cosmos.CosmosException(429, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "1")) {
                        };
                    }
                    return "OK";
                }, 1)
        ).isInstanceOfSatisfying(CosmosException.class, e -> {
            assertThat(e.getStatusCode()).isEqualTo(429);
        });

    }

    @Test
    void executeWithRetry_should_work_when_delay_time_is_minus() throws Exception {

        { // even is delay time < 0, we will work as default delay time, and do not throw exception.
            final var i = new AtomicInteger(0);

            // success within 3 second(because default wait time is 2000 ms)
            var ret = assertTimeout(ofSeconds(3), () ->
                    RetryUtil.executeWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new com.azure.cosmos.CosmosException(429, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "-1")) {
                            };
                        }
                        return "OK";
                    })
            );
            assertThat(ret).isEqualTo("OK");
        }
    }
}