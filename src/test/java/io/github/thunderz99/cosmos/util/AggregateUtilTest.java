package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AggregateUtilTest {

    @Test
    void convertToDotFieldName_should_work() {

        assertThat(AggregateUtil.convertToDotFieldName(null)).isNull();
        assertThat(AggregateUtil.convertToDotFieldName("")).isEqualTo("");
        assertThat(AggregateUtil.convertToDotFieldName("address")).isEqualTo("address");
        assertThat(AggregateUtil.convertToDotFieldName("address.city")).isEqualTo("address.city");
        assertThat(AggregateUtil.convertToDotFieldName("c['address']")).isEqualTo("address");
        assertThat(AggregateUtil.convertToDotFieldName("c['address']['city']")).isEqualTo("address.city");
        assertThat(AggregateUtil.convertToDotFieldName("c['address']['city']['street-block']")).isEqualTo("address.city.street-block");

    }

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
}