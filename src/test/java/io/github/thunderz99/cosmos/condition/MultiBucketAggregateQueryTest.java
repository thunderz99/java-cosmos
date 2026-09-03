package io.github.thunderz99.cosmos.condition;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MultiBucketAggregateQueryTest {

    @Test
    void cosmos_query_should_compile_all_buckets_with_shared_params_and_safe_aliases() {
        var aggregate = MultiBucketAggregate.of(BucketAggregateFunction.SUM, "age", List.of(
                ConditionBucket.of("bucket-with-/", Condition.filter("fullName.last", "Hanks")),
                ConditionBucket.of("another-bucket", Condition.filter(
                        "skills ARRAY_CONTAINS_ANY", List.of("Java", "Go")))
        ));

        var querySpec = Condition.filter("mail CONTAINS", "example.com")
                .limit(1)
                .toQuerySpecForMultiBucketAggregate(aggregate);

        assertThat(querySpec.queryText)
                .startsWith("SELECT SUM(")
                .contains("AS b0_matched")
                .contains("AS b0_value_count")
                .contains("SUM((((c[\"fullName\"][\"last\"] = @param001_fullName__last)) AND IS_NUMBER(c[\"age\"])) ? c[\"age\"] : undefined) AS b0")
                .contains("AS b1_matched")
                .contains("ARRAY_CONTAINS(@param002_skills, x)")
                .endsWith("WHERE (CONTAINS(c[\"mail\"], @param000_mail))")
                .doesNotContain("LIMIT 1")
                .doesNotContain("bucket-with-/");
        assertThat(querySpec.params).extracting(param -> param.name)
                .containsExactly("@param000_mail", "@param001_fullName__last", "@param002_skills");
    }

    @Test
    void cosmos_count_query_should_use_matched_column_as_value() {
        var aggregate = MultiBucketAggregate.of(BucketAggregateFunction.COUNT, "ignored", List.of(
                ConditionBucket.of("all", Condition.filter())
        ));

        var querySpec = Condition.filter().toQuerySpecForMultiBucketAggregate(aggregate);

        assertThat(querySpec.queryText).isEqualTo("SELECT SUM((true) ? 1 : 0) AS b0_matched FROM c");
    }
}
