package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleExpressionTest {

    @Test
    void buildArrayContains() {

        var params = new ArrayList<CosmosSqlParameter>();

        var queryText = SimpleExpression.buildArrayContains("skills", "@param001_skills", List.of("Java", "Python"), params);

        assertThat(queryText).isEqualTo(" (ARRAY_CONTAINS(@param001_skills, c[\"skills\"]))");
        assertThat(params).hasSize(1);

        params.forEach( p -> {
            assertThat(p.getName()).isEqualTo("@param001_skills");
            var value = p.getValue();
            assertThat(value instanceof List).isTrue();
            var set = (List) value;
            assertThat(set.get(0)).isEqualTo("Java");
            assertThat(set.get(1)).isEqualTo("Python");
        });
    }

    @Test
    void equal_to_array_should_work() {

        var selectAlias = "c";
        {
            // Array contains
            var exp = new SimpleExpression("status", List.of("A", "B"));
            var spec = exp.toQuerySpec(new AtomicInteger(0), selectAlias);
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (ARRAY_CONTAINS(@param000_status, c[\"status\"]))");
            assertThat(params).hasSize(1);

            params.forEach( p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue();
                assertThat(value instanceof List).isTrue();
                var set = (List) value;
                assertThat(set.get(0)).isEqualTo("A");
                assertThat(set.get(1)).isEqualTo("B");
            });

        }

        {
            // Array contains empty
            var exp = new SimpleExpression("status", List.of());
            var spec = exp.toQuerySpec(new AtomicInteger(0), selectAlias);
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (1=0)");
            assertThat(params).hasSize(0);

        }

        {
            // Array IN empty
            var exp = new SimpleExpression("status", List.of(), "IN");
            var spec = exp.toQuerySpec(new AtomicInteger(0), selectAlias);
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (1=0)");
            assertThat(params).hasSize(0);

        }

        {
            // Array equals
            var exp = new SimpleExpression("status", List.of("A", "B"), "=");
            var spec = exp.toQuerySpec(new AtomicInteger(0), selectAlias);
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (c[\"status\"] = @param000_status)");
            assertThat(params).hasSize(1);

            params.forEach( p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue();
                assertThat(value instanceof List).isTrue();
                var set = (List) value;
                assertThat(set.get(0)).isEqualTo("A");
                assertThat(set.get(1)).isEqualTo("B");
            });

        }

        {
            // Array equals empty
            var exp = new SimpleExpression("status", List.of(), "=");
            var spec = exp.toQuerySpec(new AtomicInteger(0), selectAlias);
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (c[\"status\"] = @param000_status)");
            assertThat(params).hasSize(1);

            params.forEach( p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue();
                assertThat(value instanceof List).isTrue();
                var set = (List) value;
                assertThat(set).hasSize(0);
            });

        }

        {
            // Array not equals empty
            var exp = new SimpleExpression("status", List.of(), "!=");
            var spec = exp.toQuerySpec(new AtomicInteger(0), selectAlias);
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (c[\"status\"] != @param000_status)");
            assertThat(params).hasSize(1);

            params.forEach(p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue();
                assertThat(value instanceof List).isTrue();
                var set = (List) value;
                assertThat(set).hasSize(0);
            });

        }
    }

}