package io.github.thunderz99.cosmos.impl.postgres.util;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class PGKeyUtilTest {


    @Test
    void getFormattedKeyWithAlias_should_work() {
        {
            // normal cases
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", "12")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", 12)).isEqualTo("NULLIF(data->>'age','')::numeric");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", Integer.MAX_VALUE)).isEqualTo("NULLIF(data->>'age','')::numeric");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", Integer.MIN_VALUE)).isEqualTo("NULLIF(data->>'age','')::numeric");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", 0)).isEqualTo("NULLIF(data->>'age','')::numeric");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", "abc")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", true)).isEqualTo("(data->>'age')::boolean");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", false)).isEqualTo("(data->>'age')::boolean");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "extra", 12)).isEqualTo("NULLIF(extra->>'age','')::numeric");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "extra", "abc")).isEqualTo("extra->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "extra", true)).isEqualTo("(extra->>'age')::boolean");
        }

        {
            // normal cases for alias = empty (afterAggregation pattern)
            // see QueryContext.afterAggregation for details
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", null, 1)).isEqualTo("\"age\"");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("address", "", true)).isEqualTo("\"address\"");
        }

        {
            // irregular cases for value
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", null)).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", "")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", " ")).isEqualTo("data->>'age'");
        }
        {
            // irregular cases for value when "value instanceof Collection<?>"
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", List.of(1, 2, 3))).isEqualTo("data->'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", Set.of(1, 2, 3))).isEqualTo("data->'age'");
        }
    }


    @Test
    void escapeKeyPart_should_work() {
        // normal cases
        assertThat(PGKeyUtil.escapeKeyPart("lastName")).isEqualTo("lastName");
        assertThat(PGKeyUtil.escapeKeyPart("address")).isEqualTo("address");

        // single quote injection cases
        assertThat(PGKeyUtil.escapeKeyPart("lastName'")).isEqualTo("lastName''");
        assertThat(PGKeyUtil.escapeKeyPart("lastName'|'")).isEqualTo("lastName''|''");
        assertThat(PGKeyUtil.escapeKeyPart("lastName'||'")).isEqualTo("lastName''||''");
        assertThat(PGKeyUtil.escapeKeyPart("key'with'quotes")).isEqualTo("key''with''quotes");

        // null case
        assertThat(PGKeyUtil.escapeKeyPart(null)).isNull();

        // empty string case
        assertThat(PGKeyUtil.escapeKeyPart("")).isEqualTo("");
    }

    @Test
    void getFormattedKeyWithAlias_should_escape_single_quotes() {
        // keys with single quotes should be escaped to prevent SQL injection
        assertThat(PGKeyUtil.getFormattedKeyWithAlias("lastName'|'", "data", "value"))
                .isEqualTo("data->>'lastName''|'''");
        assertThat(PGKeyUtil.getFormattedKeyWithAlias("address.city'inject", "data", "value"))
                .isEqualTo("data->'address'->>'city''inject'");
    }

    @Test
    void getJsonbPathKey_should_work() {
        {
            // normal cases: basic
            var pair = PGKeyUtil.getJsonbPathKey("joinPart", "joinPart.key");
            assertThat(pair.getLeft()).isEqualTo("$.\"joinPart\"[*]");
            assertThat(pair.getRight()).isEqualTo("@.\"key\"");
        }
        {   // normal cases: nested joinPart and key
            var pair = PGKeyUtil.getJsonbPathKey("join1.join2", "join1.join2.key1.key2");
            assertThat(pair.getLeft()).isEqualTo("$.\"join1\".\"join2\"[*]");
            assertThat(pair.getRight()).isEqualTo("@.\"key1\".\"key2\"");
        }

        {
            // irregular cases for joinPart
            assertThatThrownBy(() -> PGKeyUtil.getJsonbPathKey(null, "joinPart.key"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("joinPart should be non-blank");
            assertThatThrownBy(() -> PGKeyUtil.getJsonbPathKey("", "joinPart.key"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("joinPart should be non-blank");
            assertThatThrownBy(() -> PGKeyUtil.getJsonbPathKey(" ", "joinPart.key"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("joinPart should be non-blank");
        }
        {
            // irregular cases for key
            assertThatThrownBy(() -> PGKeyUtil.getJsonbPathKey("joinPart", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key should be non-blank");
            assertThatThrownBy(() -> PGKeyUtil.getJsonbPathKey("joinPart", ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key should be non-blank");
            assertThatThrownBy(() -> PGKeyUtil.getJsonbPathKey("joinPart", " "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key should be non-blank");
        }
    }

}