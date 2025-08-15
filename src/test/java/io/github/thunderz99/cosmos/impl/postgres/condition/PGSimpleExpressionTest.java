package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PGSimpleExpressionTest {

    @Test
    void toQuerySpec_should_work() {
        {
            // single value
            var expr = new PGSimpleExpression("name", "Hanks");
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (data->>'name' = @param000_name)");
            expected.addParameter(new CosmosSqlParameter("@param000_name", "Hanks"));
            assertThat(actual).isEqualTo(expected);
        }


        {
            // int value
            var expr = new PGSimpleExpression("age", 123);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (NULLIF(data->>'age','')::numeric = @param000_age)");
            expected.addParameter(new CosmosSqlParameter("@param000_age", 123));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // float value
            var expr = new PGSimpleExpression("score", 123.45f);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (NULLIF(data->>'score','')::numeric = @param000_score)");
            expected.addParameter(new CosmosSqlParameter("@param000_score", 123.45f));
            assertThat(actual).isEqualTo(expected);
        }


        {
            // double value
            var expr = new PGSimpleExpression("score", 123.45);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (NULLIF(data->>'score','')::numeric = @param000_score)");
            expected.addParameter(new CosmosSqlParameter("@param000_score", 123.45));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // boolean value
            var expr = new PGSimpleExpression("active", true);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" ((data->>'active')::boolean = @param000_active)");
            expected.addParameter(new CosmosSqlParameter("@param000_active", true));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // array value
            var list = List.of("Hanks", "Tom", "Jerry");
            var expr = new PGSimpleExpression("name", list);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (data->>'name' = ANY(@param000_name))");
            expected.addParameter(new CosmosSqlParameter("@param000_name", list));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // array value of int
            var list = List.of(1, 2, 3);
            var expr = new PGSimpleExpression("name", list);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (NULLIF(data->>'name','')::numeric = ANY(@param000_name))");
            expected.addParameter(new CosmosSqlParameter("@param000_name", list));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // array value of double
            var list = List.of(1.0, 2, 3);
            var expr = new PGSimpleExpression("name", list);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (NULLIF(data->>'name','')::numeric = ANY(@param000_name))");
            expected.addParameter(new CosmosSqlParameter("@param000_name", list));
            assertThat(actual).isEqualTo(expected);
        }

    }

    @Test
    void getTypicalValue_should_work() {
        {
            // normal cases
            assertThat(PGSimpleExpression.getTypicalValue(List.of(1,2,3))).isEqualTo(1);
            assertThat(PGSimpleExpression.getTypicalValue(List.of("a", "b"))).isEqualTo("a");
            assertThat(PGSimpleExpression.getTypicalValue(List.of(1.0, -0.2))).isEqualTo(1.0);
            assertThat(PGSimpleExpression.getTypicalValue(List.of(1L, 2L))).isEqualTo(1L);
        }

        {
            // irregular cases
            assertThat(PGSimpleExpression.getTypicalValue(List.of())).isEqualTo("");
            assertThat(PGSimpleExpression.getTypicalValue(List.of(1))).isEqualTo(1);
            assertThat(PGSimpleExpression.getTypicalValue(null)).isEqualTo("");
        }
    }

    @Test
    void buildEqualNull_should_work() {
        var expr = new PGSimpleExpression();
        
        {
            // Test with default operator (empty string) - should generate IS NULL
            var actual = expr.buildEqualNull("name", "", "data");
            var expected = " (data->'name' IS NULL)";
            assertThat(actual).isEqualTo(expected);
        }
        
        {
            // Test with != operator - should generate IS NOT NULL
            var actual = expr.buildEqualNull("name", "!=", "data");
            var expected = " (data->'name' IS NOT NULL)";
            assertThat(actual).isEqualTo(expected);
        }
        
        {
            // Test with = operator - should generate IS NULL
            var actual = expr.buildEqualNull("status", "=", "data");
            var expected = " (data->'status' IS NULL)";
            assertThat(actual).isEqualTo(expected);
        }
        
        {
            // Test with nested field
            var actual = expr.buildEqualNull("address.city", "", "data");
            var expected = " (data->'address'->'city' IS NULL)";
            assertThat(actual).isEqualTo(expected);
        }
        
        {
            // Test with different alias
            var actual = expr.buildEqualNull("name", "!=", "j1");
            var expected = " (j1->'name' IS NOT NULL)";
            assertThat(actual).isEqualTo(expected);
        }
        
        {
            // Test with deeply nested field and != operator
            var actual = expr.buildEqualNull("user.profile.settings.theme", "!=", "s1");
            var expected = " (s1->'user'->'profile'->'settings'->'theme' IS NOT NULL)";
            assertThat(actual).isEqualTo(expected);
        }
    }
}