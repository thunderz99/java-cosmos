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
    void getFormattedKey_should_work() {
        {
            // normal cases
            assertThat(PGKeyUtil.getFormattedKey("age", "abc")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKey("age", 12)).isEqualTo("(data->>'age')::int");
        }

        {
            // irregular cases for value
            Collection<?> coll = List.of("a", "b", "c");
            assertThat(PGKeyUtil.getFormattedKey("age", coll)).isEqualTo("data->'age'");

            assertThat(PGKeyUtil.getFormattedKey("age", null)).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKey("age", "")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKey("age", " ")).isEqualTo("data->>'age'");
        }
        {
            // irregular cases for value
            assertThat(PGKeyUtil.getFormattedKey("age", null)).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKey("age", "")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKey("age", " ")).isEqualTo("data->>'age'");
        }
    }

    @Test
    void getFormattedKeyWithAlias_should_work() {
        {
            // normal cases
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", "12")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", 12)).isEqualTo("(data->>'age')::int");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", Integer.MAX_VALUE)).isEqualTo("(data->>'age')::int");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", Integer.MIN_VALUE)).isEqualTo("(data->>'age')::int");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", 0)).isEqualTo("(data->>'age')::int");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", "abc")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", true)).isEqualTo("(data->>'age')::boolean");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", false)).isEqualTo("(data->>'age')::boolean");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "extra", 12)).isEqualTo("(extra->>'age')::int");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "extra", "abc")).isEqualTo("extra->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "extra", true)).isEqualTo("(extra->>'age')::boolean");
        }

        {
            // irregular cases for value
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", null)).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", "")).isEqualTo("data->>'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", " ")).isEqualTo("data->>'age'");
        }
        {
            // irregular cases for collectionAlias
            assertThatThrownBy(() -> PGKeyUtil.getFormattedKeyWithAlias("age", null, "abc"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("collectionAlias should be non-blank");
            assertThatThrownBy(() -> PGKeyUtil.getFormattedKeyWithAlias("age", "", "abc"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("collectionAlias should be non-blank");
        }
        {
            // irregular cases for value when "value instanceof Collection<?>"
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", List.of(1, 2, 3))).isEqualTo("data->'age'");
            assertThat(PGKeyUtil.getFormattedKeyWithAlias("age", "data", Set.of(1, 2, 3))).isEqualTo("data->'age'");
        }
    }

}