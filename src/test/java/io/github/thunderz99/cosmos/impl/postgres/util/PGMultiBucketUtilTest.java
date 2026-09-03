package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.List;

import io.github.thunderz99.cosmos.condition.BucketAggregateFunction;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.ConditionBucket;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PGMultiBucketUtilTest {

    @Test
    void toQuerySpec_should_generate_one_filter_query_for_all_buckets() {
        var aggregate = MultiBucketAggregate.of(BucketAggregateFunction.AVG, "age", List.of(
                ConditionBucket.of("hanks", Condition.filter("fullName.last", "Hanks")),
                ConditionBucket.of("java-or-go", Condition.filter(
                        "skills ARRAY_CONTAINS_ANY", List.of("Java", "Go")))
        ));

        var querySpec = PGMultiBucketUtil.toQuerySpec("schema1", aggregate,
                Condition.filter("mail CONTAINS", "example.com").limit(1), "table1");

        assertThat(querySpec.queryText)
                .contains("COUNT(*) FILTER (WHERE (data->'fullName'->>'last' = @param001_fullName__last)) AS \"b0_matched\"")
                .contains("AVG(NULLIF(data->>'age','')::numeric) FILTER (WHERE ((data->'fullName'->>'last' = @param001_fullName__last)) AND jsonb_typeof(data->'age') = 'number') AS \"b0\"")
                .contains("AS \"b1_matched\"")
                .contains("FROM schema1.table1")
                .endsWith("WHERE (data->>'mail' LIKE @param000_mail)")
                .doesNotContain("LIMIT 1");
        assertThat(querySpec.params).extracting(param -> param.name)
                .containsExactly("@param000_mail", "@param001_fullName__last",
                        "@param002_skills__0", "@param002_skills__1");
    }
}
