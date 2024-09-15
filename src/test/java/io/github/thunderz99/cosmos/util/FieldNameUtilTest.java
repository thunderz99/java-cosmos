package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FieldNameUtilTest {

    @Test
    void convertToDotFieldName_should_work() {

        assertThat(FieldNameUtil.convertToDotFieldName(null)).isNull();
        assertThat(FieldNameUtil.convertToDotFieldName("")).isEqualTo("");
        assertThat(FieldNameUtil.convertToDotFieldName("address")).isEqualTo("address");
        assertThat(FieldNameUtil.convertToDotFieldName("address.city")).isEqualTo("address.city");
        assertThat(FieldNameUtil.convertToDotFieldName("c['address']")).isEqualTo("address");
        assertThat(FieldNameUtil.convertToDotFieldName("c[\"address\"][\"city\"]")).isEqualTo("address.city");
        assertThat(FieldNameUtil.convertToDotFieldName("c['address']['city']['street-block']")).isEqualTo("address.city.street-block");
        assertThat(FieldNameUtil.convertToDotFieldName("c[\"address\"][\"city\"]['street-block']")).isEqualTo("address.city.street-block");

    }

}