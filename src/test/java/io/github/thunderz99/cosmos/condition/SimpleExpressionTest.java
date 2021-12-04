package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.azure.cosmos.models.SqlParameter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SimpleExpressionTest {

    @Test
    void buildArrayContains() {

        var params = new ArrayList<SqlParameter>();

        var queryText = SimpleExpression.buildArrayContains("skills", "@param001_skills", List.of("Java", "Python"), params);

        assertThat(queryText).isEqualTo(" (ARRAY_CONTAINS(@param001_skills, c[\"skills\"]))");
        assertThat(params).hasSize(1);

        params.forEach( p -> {
            assertThat(p.getName()).isEqualTo("@param001_skills");
            var value = p.getValue(List.class);
            assertThat(value instanceof List<?>).isTrue();
            var set = (List<?>) value;
            assertThat(set.get(0)).isEqualTo("Java");
            assertThat(set.get(1)).isEqualTo("Python");
        });
    }

    @Test
    void equal_to_array_should_work() {

        {
            // Array contains
            var exp = new SimpleExpression("status", List.of("A", "B"));
            var spec = exp.toQuerySpec(new AtomicInteger(0));
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (ARRAY_CONTAINS(@param000_status, c[\"status\"]))");
            assertThat(params).hasSize(1);

            params.forEach( p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue(List.class);
                assertThat(value instanceof List<?>).isTrue();
                var set = (List<?>) value;
                assertThat(set.get(0)).isEqualTo("A");
                assertThat(set.get(1)).isEqualTo("B");
            });

        }

        {
            // Array contains empty
            var exp = new SimpleExpression("status", List.of());
            var spec = exp.toQuerySpec(new AtomicInteger(0));
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (1=0)");
            assertThat(params).hasSize(0);

        }

        {
            // Array IN empty
            var exp = new SimpleExpression("status", List.of(), "IN");
            var spec = exp.toQuerySpec(new AtomicInteger(0));
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (1=0)");
            assertThat(params).hasSize(0);

        }

        {
            // Array equals
            var exp = new SimpleExpression("status", List.of("A", "B"), "=");
            var spec = exp.toQuerySpec(new AtomicInteger(0));
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (c[\"status\"] = @param000_status)");
            assertThat(params).hasSize(1);

            params.forEach( p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue(List.class);
                assertThat(value instanceof List<?>).isTrue();
                var set = (List<?>) value;
                assertThat(set.get(0)).isEqualTo("A");
                assertThat(set.get(1)).isEqualTo("B");
            });

        }

        {
            // Array equals empty
            var exp = new SimpleExpression("status", List.of(), "=");
            var spec = exp.toQuerySpec(new AtomicInteger(0));
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (c[\"status\"] = @param000_status)");
            assertThat(params).hasSize(1);

            params.forEach( p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue(List.class);
                assertThat(value instanceof List<?>).isTrue();
                var set = (List<?>) value;
                assertThat(set).isEmpty();
            });

        }

        {
            // Array not equals empty
            var exp = new SimpleExpression("status", List.of(), "!=");
            var spec = exp.toQuerySpec(new AtomicInteger(0));
            var queryText = spec.getQueryText();
            var params = spec.getParameters();

            assertThat(queryText).isEqualTo(" (c[\"status\"] != @param000_status)");
            assertThat(params).hasSize(1);

            params.forEach(p -> {
                assertThat(p.getName()).isEqualTo("@param000_status");
                var value = p.getValue(List.class);
                assertThat(value instanceof List<?>).isTrue();
                var set = (List<?>) value;
                assertThat(set).isEmpty();
            });

        }
    }

    @Test
    void get_param_name_should_work() {

        // normal
        assertThat(SimpleExpression.getParamNameFromKey("name", 0)).isEqualTo("@param000_name");
        assertThat(SimpleExpression.getParamNameFromKey("fullName.last", 1)).isEqualTo("@param001_fullName__last");
        assertThat(SimpleExpression.getParamNameFromKey("829cc727-2d49-4d60-8f91-b30f50560af7.name", 1)).matches("@param001_[\\d\\w]{7}__name");
        assertThat(SimpleExpression.getParamNameFromKey("family.テスト.age", 2)).matches("@param002_family__[\\d\\w]{7}__age");
        assertThat(SimpleExpression.getParamNameFromKey("aa-bb", 2)).matches("@param002_[\\d\\w]{7}");

        // abnormal
        assertThatThrownBy(() -> {
            SimpleExpression.getParamNameFromKey("", 1);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key").hasMessageContaining("should be non-blank");

        assertThatThrownBy(() -> {
            SimpleExpression.getParamNameFromKey(null, 2);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key").hasMessageContaining("should be non-blank");

        assertThat(SimpleExpression.getParamNameFromKey("name", -1)).isEqualTo("@param-01_name");
    }

}