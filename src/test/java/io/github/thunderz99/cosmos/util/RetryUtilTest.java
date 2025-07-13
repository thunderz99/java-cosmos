package io.github.thunderz99.cosmos.util;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import com.azure.cosmos.CosmosDiagnostics;
import com.azure.cosmos.CosmosItemSerializer;
import com.azure.cosmos.implementation.CosmosError;
import com.azure.cosmos.models.*;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosBatchResponseWrapper;
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

            var tracker = LogTracker.getInstance(RetryUtil.class);

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

            // assert that the WARN level log is logged
            assertThat(tracker.getEvents()).anyMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("RetryUtil 429 occurred. statusCode:429, wait:10 ms"));
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

            var tracker = LogTracker.getInstance(RetryUtil.class);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new CosmosException(408, "REQUEST_TIMEOUT", "Request Timeout", 5);
                        }
                        return "OK";
                    })
            );

            assertThat(tracker.getEvents()).anyMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("RetryUtil 429 occurred. statusCode:408, wait:5 ms"));
            assertThat(ret).isEqualTo("OK");

        }
    }

    @Test
    void executeWithRetry_should_throw_exception_after_max_retries() throws Exception {


        // throw exception after 10 retries
        final var i = new AtomicInteger(0);

        var tracker = LogTracker.getInstance(RetryUtil.class);

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

        assertThat(tracker.getEvents()).anyMatch(e -> e.getLevel() == Level.WARN
                && e.getFormattedMessage().contains("RetryUtil exceeded max retries. statusCode:429"));

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

    @Test
    void executeBatchWithRetry_should_not_retry_401() throws Exception {

        { // should not retry 401 for status in return value
            var tracker = LogTracker.getInstance(RetryUtil.class);
            assertThatThrownBy(() ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        return new CosmosBatchResponseWrapper(401, 10, "Unauthenticated");
                    })
            ).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                assertThat(e.getStatusCode()).isEqualTo(401);
                assertThat(e.getCode()).isEqualTo("10");
                assertThat(e.getMessage()).isEqualTo("Unauthenticated");
            });
            assertThat(tracker.getEvents()).anyMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("Exception should not retry occurred. statusCode:401"));
            assertThat(tracker.getEvents()).anyMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("executeBatchWithRetry response not success. statusCode:401"));

        }


        { // should not retry 401 for CosmosException
            var tracker = LogTracker.getInstance(RetryUtil.class);
            assertThatThrownBy(() ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        throw new com.azure.cosmos.CosmosException(401, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "10")) {
                        };
                    })
            ).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                assertThat(e.getStatusCode()).isEqualTo(401);
            });
            assertThat(tracker.getEvents()).anyMatch(e -> e.getLevel() == Level.WARN
                    && e.getFormattedMessage().contains("Exception should not retry occurred. statusCode:401"));
        }


    }

    @Test
    void executeBatchWithRetry_should_succeed_with_max_retries() throws Exception {

        { // success
            var ret = RetryUtil.executeBatchWithRetry(() -> {
                return new CosmosBatchResponseWrapper(200, 11, "OK");
            });
            assertThat(ret.getStatusCode()).isEqualTo(200);
        }

        { // success after 2 retries. for 429
            final var i = new AtomicInteger(0);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            return new CosmosBatchResponseWrapper(429, 10, "Too many requests", Duration.ofMillis(1));
                        }
                        return new CosmosBatchResponseWrapper(200, 11, "OK");
                    })
            );
            assertThat(ret.getErrorMessage()).isEqualTo("OK");
        }

        { // success after 3 retries. for 408
            final var i = new AtomicInteger(0);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        if (i.incrementAndGet() < 3) {
                            return new CosmosBatchResponseWrapper(408, 10, "Request timeout", Duration.ofMillis(1));
                        }
                        return new CosmosBatchResponseWrapper(200, 11, "OK");
                    })
            );
            assertThat(ret.getErrorMessage()).isEqualTo("OK");
        }

        { // success after 2 retries. for 449
            final var i = new AtomicInteger(0);

            // success within 1 second
            var ret = assertTimeout(ofSeconds(1), () ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        if (i.incrementAndGet() < 2) {
                            throw new com.azure.cosmos.CosmosException(449, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "10")) {
                            };
                        }
                        return new CosmosBatchResponseWrapper(200, 11, "OK");
                    })
            );
            assertThat(ret.getErrorMessage()).isEqualTo("OK");
        }
    }

    @Test
    void executeBatchWithRetry_should_throw_exception_after_max_retries() throws Exception {

        { // exception after 3 retries. for 429 CosmosException
            final var i = new AtomicInteger(0);

            assertThatThrownBy(() ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        if (i.incrementAndGet() < 5) {
                            throw new com.azure.cosmos.CosmosException(429, new CosmosError("{}"), Map.of(RETRY_AFTER_IN_MILLISECONDS, "10")) {
                            };
                        }
                        return new CosmosBatchResponseWrapper(200, 11, "OK");
                    })
            ).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                assertThat(e.getStatusCode()).isEqualTo(429);
            });
        }

        { // exception after 3 retries. for 449
            final var i = new AtomicInteger(0);

            // success within 1 second
            assertThatThrownBy(() ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        if (i.incrementAndGet() < 5) {
                            return new CosmosBatchResponseWrapper(449, 10, "Retry With", Duration.ofMillis(1));
                        }
                        return new CosmosBatchResponseWrapper(200, 11, "OK");
                    })
            ).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                assertThat(e.getStatusCode()).isEqualTo(449);
                assertThat(e.getMessage()).isEqualTo("Retry With");

            });
        }

        { // exception after 3 retries. for 408
            final var i = new AtomicInteger(0);

            // success within 1 second
            assertThatThrownBy(() ->
                    RetryUtil.executeBatchWithRetry(() -> {
                        if (i.incrementAndGet() < 5) {
                            return new CosmosBatchResponseWrapper(408, 10, "Request Timeout", Duration.ofMillis(1));
                        }
                        return new CosmosBatchResponseWrapper(200, 11, "OK");
                    })
            ).isInstanceOfSatisfying(CosmosException.class, (e) -> {
                assertThat(e.getStatusCode()).isEqualTo(408);
                assertThat(e.getMessage()).isEqualTo("Request Timeout");

            });
        }

    }

    public static class User {
        public String id;
        public String firstName;
        public String lastName;

        public String createdAt;

        public User() {
        }

        public User(String id, String firstName, String lastName) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
        }

        public User(String id, String firstName, String lastName, String createdAt) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.createdAt = createdAt;
        }
    }

    @Test
    void executeBulkWithRetry_should_work() throws Exception {
        var tracker = LogTracker.getInstance(RetryUtil.class, Level.INFO);
        var partition = "Users";
        var coll = "coll_executeBulkWithRetry_should_work";

        var data = new ArrayList<User>(10);
        for (int i = 0; i < 10; i++) {
            data.add(new User("doBulkWithRetry_should_work_" + i, "first" + i, "last" + i));
        }

        var partitionKey = new PartitionKey(partition);
        var operations = data.stream().map(it -> {
                    var map = JsonUtil.toMap(it);
                    map.put(Cosmos.getDefaultPartitionKey(), partition);
                    return CosmosBulkOperations.getCreateItemOperation(map, partitionKey);
                }
        ).collect(Collectors.toList());

        var wait = 5;
        final var i = new AtomicInteger(0);
        RetryUtil.executeBulkWithRetry(coll, operations,
                (ops) -> {
                    if (i.get() < 2) {
                        i.incrementAndGet();
                        var iter = ops.iterator();
                        return List.of(generateCosmosBulkOperationResponse(iter.next(),
                                generateCosmosBulkItemResponse(429, 5),
                                new CosmosException(429, "429", "Too Many Requests", 5)));
                    }
                    return List.of();
                });

        // check retry logic is called in WARN level
        var events = tracker.getEvents().stream().filter(e -> e.getLevel() == Level.WARN).collect(Collectors.toList());
        assertThat(events.get(0).toString()).isEqualTo(String.format("[WARN] doBulkWithRetry 429 occurred. Code:429, coll:%s, partition:[\"%s\"]. operationType:CREATE, Wait:%d ms", coll, partition, wait));

        // wait should be doubled
        assertThat(events.get(1).toString()).isEqualTo(String.format("[WARN] doBulkWithRetry 429 occurred. Code:429, coll:%s, partition:[\"%s\"]. operationType:CREATE, Wait:%d ms", coll, partition, wait * 2));
    }


    /**
     * use reflect to generate CosmosBulkItemResponse for test(which is not accessible directly)
     * @param statusCode
     * @param retryAfter
     * @return
     * @throws Exception
     */
    static CosmosBulkItemResponse generateCosmosBulkItemResponse(int statusCode, int retryAfter) throws Exception {
        // Use reflection to access the package-private constructor
        Constructor<CosmosBulkItemResponse> constructor =
                CosmosBulkItemResponse.class.getDeclaredConstructor(
                        String.class,
                        double.class,
                        ObjectNode.class,
                        int.class,
                        Duration.class,
                        int.class,
                        Map.class,
                        CosmosDiagnostics.class,
                        CosmosItemSerializer.class);

        // Make the constructor accessible
        constructor.setAccessible(true);

        // Create an instance of CosmosBulkItemResponse using the constructor
        CosmosBulkItemResponse response = constructor.newInstance(
                "etagValue",                        // eTag
                1.0,                                // requestCharge
                JsonNodeFactory.instance.objectNode(), // resourceObject
                statusCode,                                // statusCode
                Duration.ofMillis(retryAfter),             // retryAfter
                0,                                  // subStatusCode
                new HashMap<>(),                    // responseHeaders
                null,                               // cosmosDiagnostics
                null                                // effectiveItemSerializer
        );

        return response;

    }


    static CosmosBulkOperationResponse generateCosmosBulkOperationResponse(CosmosItemOperation op, CosmosBulkItemResponse itemResponse, Exception e) throws Exception {
        // Use reflection to access the package-private constructor
        Constructor<CosmosBulkOperationResponse> constructor =
                CosmosBulkOperationResponse.class.getDeclaredConstructor(
                        CosmosItemOperation.class, // Operation
                        CosmosBulkItemResponse.class, // Response
                        Exception.class, // Exception
                        Object.class // BatchContext (TContext)
                );

        // Make the constructor accessible
        constructor.setAccessible(true);


        // Create an instance of CosmosBulkOperationResponse using the constructor
        CosmosBulkOperationResponse<String> response = constructor.newInstance(
                op,
                itemResponse,
                e,
                null
        );

        return response;
    }

    @Test
    void calculateWaitTime_should_work() throws Exception {
        {
            // normal cases
            assertThat(RetryUtil.calculateWaitTime(2000, 1)).isEqualTo(2000L);
            assertThat(RetryUtil.calculateWaitTime(2000, 2)).isEqualTo(4000L);
            assertThat(RetryUtil.calculateWaitTime(2000, 3)).isEqualTo(8000L);
            assertThat(RetryUtil.calculateWaitTime(1000, 1)).isEqualTo(1000L);
            assertThat(RetryUtil.calculateWaitTime(2000, 10)).isEqualTo(RetryUtil.SINGLE_EXECUTION_MAX_WAIT_TIME);

            // irregular cases
            assertThat(RetryUtil.calculateWaitTime(1, 1)).isEqualTo(1L);
            assertThat(RetryUtil.calculateWaitTime(-1, 1)).isEqualTo(2000L);
            assertThat(RetryUtil.calculateWaitTime(0, 1)).isEqualTo(2000L);
            assertThat(RetryUtil.calculateWaitTime(2000, 0)).isEqualTo(1000L);
            assertThat(RetryUtil.calculateWaitTime(2000, -1)).isEqualTo(500L);
        }
    }

}