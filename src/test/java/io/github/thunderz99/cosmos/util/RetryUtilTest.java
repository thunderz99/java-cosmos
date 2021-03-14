package io.github.thunderz99.cosmos.util;

import com.microsoft.azure.documentdb.DocumentClientException;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.microsoft.azure.documentdb.internal.HttpConstants.HttpHeaders.RETRY_AFTER_IN_MILLISECONDS;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

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

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                RetryUtil.executeWithRetry(() -> {
                    if(i.incrementAndGet() < 2){
                        throw new DocumentClientException(429, new com.microsoft.azure.documentdb.Error("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "10"));
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
                if(i.incrementAndGet() < 12){
                    throw new DocumentClientException(429);
                }
                return "OK";
            }, 1)
        ).isInstanceOfSatisfying(DocumentClientException.class, e -> {
            assertThat(e.getStatusCode()).isEqualTo(429);
        });

    }
}