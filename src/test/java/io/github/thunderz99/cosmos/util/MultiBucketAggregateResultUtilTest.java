package io.github.thunderz99.cosmos.util;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;

import io.github.thunderz99.cosmos.condition.BucketAggregateFunction;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.ConditionBucket;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregate;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiBucketAggregateResultUtilTest {

    @Test
    void fromFlatRows_should_preserve_order_count_and_null_semantics() {
        var aggregate = MultiBucketAggregate.of(BucketAggregateFunction.SUM, "age", List.of(
                ConditionBucket.of("bucket/2", Condition.filter()),
                ConditionBucket.of("bucket-1", Condition.filter())
        ));
        var row = new LinkedHashMap<String, Object>();
        row.put("b0_matched", 2L);
        row.put("b0_value_count", 2L);
        row.put("b0", new BigDecimal("42"));
        row.put("b1_matched", 1);
        row.put("b1_value_count", 0L);
        row.put("b1", 0.0);

        var results = MultiBucketAggregateResultUtil.fromFlatRows(aggregate, List.of(row));

        assertThat(results).extracting(result -> result.bucketId)
                .containsExactly("bucket/2", "bucket-1");
        assertThat(results.get(0).matched).isEqualTo(2L);
        assertThat(results.get(0).value).isEqualTo(new BigDecimal("42"));
        assertThat(results.get(1).matched).isEqualTo(1L);
        assertThat(results.get(1).value).isNull();
    }

    @Test
    void fromFlatRows_should_derive_count_value_and_reject_missing_columns() {
        var count = MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null,
                List.of(ConditionBucket.of("count", Condition.filter())));
        var countResults = MultiBucketAggregateResultUtil.fromFlatRows(count,
                List.of(new LinkedHashMap<>(java.util.Map.of("b0_matched", 3))));
        assertThat(countResults.get(0).matched).isEqualTo(3L);
        assertThat(countResults.get(0).value).isEqualTo(3L);

        var sum = MultiBucketAggregate.of(BucketAggregateFunction.SUM, "age",
                List.of(ConditionBucket.of("sum", Condition.filter())));
        assertThatThrownBy(() -> MultiBucketAggregateResultUtil.fromFlatRows(sum,
                List.of(new LinkedHashMap<>(java.util.Map.of("b0_matched", 0)))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Missing aggregate result column: b0");

        var invalidNullRow = new LinkedHashMap<String, Object>();
        invalidNullRow.put("b0_matched", 1);
        invalidNullRow.put("b0_value_count", 1);
        invalidNullRow.put("b0", null);
        assertThatThrownBy(() -> MultiBucketAggregateResultUtil.fromFlatRows(sum, List.of(invalidNullRow)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("is null although b0_value_count is 1");
    }
}
