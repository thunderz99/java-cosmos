package io.github.thunderz99.cosmos.impl.postgres.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.*;

class PGSortUtilTest {

    @Test
    void getFormattedKey4Sort_should_work() {

        {
            // normal cases
            assertThat(PGSortUtil.getFormattedKey4Sort("name", "ASC", "en_US")).isEqualTo("data->>'name' ASC");
            assertThat(PGSortUtil.getFormattedKey4Sort("name", "DESC", "C")).isEqualTo("data->>'name' COLLATE \"C\" DESC");
            assertThat(PGSortUtil.getFormattedKey4Sort("address.city", "ASC", "en_US")).isEqualTo(" data->'address'->'city' ASC");
            assertThat(PGSortUtil.getFormattedKey4Sort("age", "DESC", "en_US")).isEqualTo(" data->'age' DESC");
            assertThat(PGSortUtil.getFormattedKey4Sort("age::int", "DESC", "en_US")).isEqualTo("(data->>'age')::int DESC");
        }

        {
            // irregular cases
            assertThatThrownBy(() -> PGSortUtil.getFormattedKey4Sort("name", "ASC", "wrong collate"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("collate should be \"C\" or \"en_US\"");
        }

    }
}