package io.github.thunderz99.cosmos.util;

import java.util.ArrayList;
import java.util.Map;

import io.github.thunderz99.cosmos.condition.Aggregate;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AggregateUtilTest {
    
    @Test
    void extractFunctionAndAlias_should_work() {
        {
            // irregular
            var pair = AggregateUtil.extractFunctionAndAlias(null);
            assertThat(pair.getLeft()).isEmpty();
            assertThat(pair.getRight()).isEmpty();

            pair = AggregateUtil.extractFunctionAndAlias("");
            assertThat(pair.getLeft()).isEmpty();
            assertThat(pair.getRight()).isEmpty();
        }

        {
            // single quotation should be removed for alias
            var pair = AggregateUtil.extractFunctionAndAlias("COUNT(1) AS 'count'");
            assertThat(pair.getLeft()).isEqualTo("COUNT(1)");
            assertThat(pair.getRight()).isEqualTo("count");
        }

        {
            // nested aggregation should work
            var pair = AggregateUtil.extractFunctionAndAlias("SUM(ARRAY_LENGTH(c['children'])) AS 'childrenCount'");
            assertThat(pair.getLeft()).isEqualTo("SUM(ARRAY_LENGTH(c['children']))");
            assertThat(pair.getRight()).isEqualTo("childrenCount");
        }


    }

    @Test
    void extractFieldFromFunction_should_work() {
        assertThat(AggregateUtil.extractFieldFromFunction("SUM(c['children'])")).isEqualTo("c['children']");
        assertThat(AggregateUtil.extractFieldFromFunction("avg(c['address']['state'])")).isEqualTo("c['address']['state']");
        assertThat(AggregateUtil.extractFieldFromFunction("MIN(c.age)")).isEqualTo("c.age");
        assertThat(AggregateUtil.extractFieldFromFunction("max(test.score)")).isEqualTo("test.score");
        assertThat(AggregateUtil.extractFieldFromFunction("count(1)")).isEqualTo("1");
        assertThat(AggregateUtil.extractFieldFromFunction("test.score")).isEqualTo("test.score");
        assertThat(AggregateUtil.extractFieldFromFunction("c['address']['state']")).isEqualTo("c['address']['state']");
    }

    @Test
    void processEmptyAggregateResults_should_work() {

        // count
        assertThat(AggregateUtil.processEmptyAggregateResults(Aggregate.function("COUNT(1) AS count"), new ArrayList<>()))
                .hasSize(1).contains(new Document("count", 0));

        // count
        assertThat(AggregateUtil.processEmptyAggregateResults(Aggregate.function("COUNT(c.age) AS count"), new ArrayList<>()))
                .hasSize(1).contains(new Document("count", 0));

        // max
        assertThat(AggregateUtil.processEmptyAggregateResults(Aggregate.function("max(c.age) AS count"), new ArrayList<>()))
                .hasSize(1).contains(new Document("count", Map.of()));

        // nested
        assertThat(AggregateUtil.processEmptyAggregateResults(Aggregate.function("SUM(ARRAY_LENGTH(c.children)) AS count"), new ArrayList<>()))
                .hasSize(1).contains(new Document("count", Map.of()));

        // simple field without aggregation
        assertThat(AggregateUtil.processEmptyAggregateResults(Aggregate.function("c.children"), new ArrayList<>()))
                .hasSize(0);

        // simple field without aggregation
        assertThat(AggregateUtil.processEmptyAggregateResults(Aggregate.function("c['address']['state']"), new ArrayList<>()))
                .hasSize(0);

    }
}