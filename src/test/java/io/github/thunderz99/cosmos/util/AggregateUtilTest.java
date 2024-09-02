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
}