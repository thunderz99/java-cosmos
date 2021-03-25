package io.github.thunderz99.cosmos.condition;

import com.microsoft.azure.documentdb.SqlParameterCollection;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class SimpleExpressionTest {

    @Test
    void buildArrayContains() {

        var params = new SqlParameterCollection();

        var queryText = SimpleExpression.buildArrayContains("skills", "@param001_skills", List.of("Java", "Python"), params);

        assertThat(queryText).isEqualTo(" (ARRAY_CONTAINS(@param001_skills, c[\"skills\"]))");
        assertThat(params).hasSize(1);

        params.forEach( p -> {
            assertThat(p.getName()).isEqualTo("@param001_skills");
            var value = p.get("value");
            assertThat(value instanceof JSONArray).isTrue();
            var set = (JSONArray) value;
            assertThat(set.get(0)).isEqualTo("Java");
            assertThat(set.get(1)).isEqualTo("Python");
        });
    }
}