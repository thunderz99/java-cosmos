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
            // array value
            var expr = new PGSimpleExpression("name", List.of("Hanks", "Tom", "Jerry"));
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" (data->>'name' = ANY(@param000_name))");
            expected.addParameter(new CosmosSqlParameter("@param000_name", List.of("Hanks", "Tom", "Jerry")));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // int value
            var expr = new PGSimpleExpression("age", 123);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" ((data->>'age')::int = @param000_age)");
            expected.addParameter(new CosmosSqlParameter("@param000_age", 123));
            assertThat(actual).isEqualTo(expected);
        }

        {
            // float value
            var expr = new PGSimpleExpression("score", 123.45f);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" ((data->>'score')::float8 = @param000_score)");
            expected.addParameter(new CosmosSqlParameter("@param000_score", 123.45f));
            assertThat(actual).isEqualTo(expected);
        }


        {
            // double value
            var expr = new PGSimpleExpression("score", 123.45);
            var actual = expr.toQuerySpec(new AtomicInteger(0), "data");
            var expected = new CosmosSqlQuerySpec();
            expected.setQueryText(" ((data->>'score')::float8 = @param000_score)");
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

    }
}