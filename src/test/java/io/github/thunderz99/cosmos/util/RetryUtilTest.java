package io.github.thunderz99.cosmos.util;

import java.util.concurrent.atomic.AtomicInteger;

import com.azure.cosmos.implementation.RequestRateTooLargeException;
import com.azure.cosmos.implementation.http.HttpHeaders;
import org.junit.jupiter.api.Test;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeout;

class RetryUtilTest {

    @Test
    void executeWithRetry_should_succeed_with_max_retries() throws Exception {

        { // success
            var ret = RetryUtil.executeWithRetry(() -> {
                return "OK";
            });
            assertThat(ret).isEqualTo("OK");
        }

        { // success after 2 retries
            final var i = new AtomicInteger(0);

            var headers = new HttpHeaders();
            headers.set("x-ms-retry-after-ms", "10");

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new RequestRateTooLargeException("test", headers, null);
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
                        throw new RequestRateTooLargeException();
                    }
                    return "OK";
                }, 1)
        ).isInstanceOfSatisfying(RequestRateTooLargeException.class, e -> {
            assertThat(e.getStatusCode()).isEqualTo(429);
        });

    }
}