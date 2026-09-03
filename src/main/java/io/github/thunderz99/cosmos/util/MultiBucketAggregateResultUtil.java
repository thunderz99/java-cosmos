package io.github.thunderz99.cosmos.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.github.thunderz99.cosmos.condition.BucketAggregateFunction;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregate;
import io.github.thunderz99.cosmos.dto.MultiBucketAggregateResult;

/**
 * Strictly maps a flat, aliased database row back to caller-owned bucket identifiers.
 */
public final class MultiBucketAggregateResultUtil {

    private MultiBucketAggregateResultUtil() {
    }

    public static List<MultiBucketAggregateResult> fromFlatRows(MultiBucketAggregate aggregate,
                                                                 List<? extends Map<String, Object>> rows) {
        if (rows == null || rows.size() != 1) {
            throw new IllegalStateException("Multi-bucket aggregate expected exactly one result row but got "
                    + (rows == null ? 0 : rows.size()));
        }

        var row = rows.get(0);
        var results = new ArrayList<MultiBucketAggregateResult>(aggregate.buckets.size());
        for (var i = 0; i < aggregate.buckets.size(); i++) {
            var alias = "b" + i;
            var matchedAlias = alias + "_matched";
            if (!row.containsKey(matchedAlias)) {
                throw new IllegalStateException("Missing aggregate result column: " + matchedAlias);
            }

            var matched = toLong(row.get(matchedAlias), matchedAlias);
            Number value;
            if (aggregate.function == BucketAggregateFunction.COUNT) {
                value = matched;
            } else {
                var valueCountAlias = alias + "_value_count";
                var hasValueCount = row.containsKey(valueCountAlias);
                var valueCount = hasValueCount ? toLong(row.get(valueCountAlias), valueCountAlias) : -1L;
                if (hasValueCount && valueCount == 0) {
                    value = null;
                } else if (!row.containsKey(alias)) {
                    throw new IllegalStateException("Missing aggregate result column: " + alias);
                } else {
                    value = toNullableNumber(row.get(alias), alias);
                    if (hasValueCount && valueCount > 0 && value == null) {
                        throw new IllegalStateException("Aggregate result column %s is null although %s is %d"
                                .formatted(alias, valueCountAlias, valueCount));
                    }
                }
            }
            results.add(new MultiBucketAggregateResult(aggregate.buckets.get(i).bucketId, matched, value));
        }
        return results;
    }

    private static long toLong(Object value, String alias) {
        if (value == null) {
            return 0L;
        }
        if (!(value instanceof Number number)) {
            throw new IllegalStateException("Aggregate result column %s is not numeric: %s".formatted(alias, value));
        }
        try {
            return new BigDecimal(number.toString()).longValueExact();
        } catch (ArithmeticException e) {
            throw new IllegalStateException("Aggregate result column %s is not an integer: %s".formatted(alias, value), e);
        }
    }

    private static Number toNullableNumber(Object value, String alias) {
        if (value == null || value instanceof Map<?, ?> map && map.isEmpty()) {
            return null;
        }
        if (!(value instanceof Number number)) {
            throw new IllegalStateException("Aggregate result column %s is not numeric: %s".formatted(alias, value));
        }
        return number;
    }
}
