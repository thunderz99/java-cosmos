package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NamedParameterUtilTest {

    @Test
    void convert_should_work() {
        var sql = "SELECT * FROM c WHERE c.id = @id AND c.name = @name";
        Map<String, Object> paramMap = Map.of("@id", 1, "@name", "Tom Banks");

        {
            var result = NamedParameterUtil.convert(sql, paramMap);
            assertThat(result.getQueryText()).isEqualTo("SELECT * FROM c WHERE c.id = ? AND c.name = ?");
            assertThat(result.getParameters().size()).isEqualTo(2);
            assertThat(result.getParameters().get(0).getValue()).isEqualTo(1);
            assertThat(result.getParameters().get(1).getValue()).isEqualTo(paramMap.get("@name"));
        }

        // irregular cases
        {
            assertThatThrownBy(() -> NamedParameterUtil.convert(null, paramMap))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sql should not be null");
            assertThatThrownBy(() -> NamedParameterUtil.convert(sql, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("paramMap should not be null");
        }

    }
}