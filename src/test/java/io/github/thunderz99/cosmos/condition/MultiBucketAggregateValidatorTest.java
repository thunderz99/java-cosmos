package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.List;

import io.github.thunderz99.cosmos.QueryTooComplexException;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiBucketAggregateValidatorTest {

    @Test
    void validate_should_accept_nested_basic_and_nested_conditions() {
        var condition = Condition.filter(
                "$OR", List.of(
                        Condition.filter("status", "active"),
                        Condition.filter("age >=", 20)
                ),
                "$NOT", Condition.filter("name CONTAINS", "test")
        );
        var aggregate = MultiBucketAggregate.of(BucketAggregateFunction.SUM, "fullName.score",
                List.of(ConditionBucket.of("bucket/1", condition)));

        MultiBucketAggregateValidator.validate(aggregate, Condition.filter("tenantId", "tenant-1"));

        assertThat(MultiBucketAggregateValidator.normalizeField("c.fullName.score"))
                .isEqualTo("fullName.score");
    }

    @Test
    void validate_should_reject_invalid_inputs_before_compilation() {
        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null, List.of()), Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("buckets");

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.SUM, " ",
                        List.of(ConditionBucket.of("b1", Condition.filter()))), Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field");

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null, List.of(
                        ConditionBucket.of("duplicate", Condition.filter()),
                        ConditionBucket.of("duplicate", Condition.filter()))), Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate bucketId");

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null,
                        List.of(ConditionBucket.of("b1",
                                Condition.filter("assignedOrgIds ARRAY_CONTAINS_ANY", List.of())))),
                Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty ARRAY_CONTAINS_ANY");

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null,
                        List.of(ConditionBucket.of("b1",
                                Condition.filter("assignedOrgIds ARRAY_CONTAINS_ALL", List.of())))),
                Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("empty ARRAY_CONTAINS_ALL");

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null,
                        List.of(ConditionBucket.of("b1", Condition.rawSql("1=1")))), Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("raw SQL");

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null,
                        List.of(ConditionBucket.of("b1", Condition.filter("$EXPRESSION", "c.age > 1")))),
                Condition.filter()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported condition");
    }

    @Test
    void validate_should_reject_requests_over_limits() {
        var buckets = new ArrayList<ConditionBucket>();
        for (var i = 0; i <= MultiBucketAggregateValidator.MAX_BUCKETS; i++) {
            buckets.add(ConditionBucket.of("b" + i, Condition.filter()));
        }

        assertThatThrownBy(() -> MultiBucketAggregateValidator.validate(
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null, buckets), Condition.filter()))
                .isInstanceOf(QueryTooComplexException.class)
                .hasMessageContaining("Bucket count");

        var oversizedQuery = "x".repeat(MultiBucketAggregateValidator.MAX_REQUEST_BYTES + 1);
        assertThatThrownBy(() -> MultiBucketAggregateValidator.checkCompiledRequest(oversizedQuery, List.of()))
                .isInstanceOf(QueryTooComplexException.class)
                .hasMessageContaining("request size");

        var oversizedParameter = new CosmosSqlParameter("@p0",
                "x".repeat(MultiBucketAggregateValidator.MAX_REQUEST_BYTES + 1));
        assertThatThrownBy(() -> MultiBucketAggregateValidator.checkCompiledRequest("SELECT 1",
                List.of(oversizedParameter)))
                .isInstanceOf(QueryTooComplexException.class)
                .hasMessageContaining("request size");
    }
}
