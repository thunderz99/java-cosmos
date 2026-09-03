package io.github.thunderz99.cosmos.condition;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.thunderz99.cosmos.QueryTooComplexException;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.util.FieldNameUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * Fail-fast validation and complexity limits shared by all multi-bucket implementations.
 */
public final class MultiBucketAggregateValidator {

    public static final int MAX_BUCKETS = 100;
    public static final int MAX_COMPILED_PARAMS = 10_000;
    public static final int MAX_REQUEST_BYTES = 512 * 1024;

    private static final Set<String> ALLOWED_OPERATORS = Set.of(
            "=", "!=", "<", "<=", ">", ">=",
            "LIKE", "STARTSWITH", "ENDSWITH", "CONTAINS", "IN",
            "IS_DEFINED", "IS_NULL",
            "ARRAY_CONTAINS", "ARRAY_CONTAINS_ANY", "ARRAY_CONTAINS_ALL"
    );

    private MultiBucketAggregateValidator() {
    }

    /**
     * Validate the complete request without accessing a database.
     */
    public static void validate(MultiBucketAggregate aggregate, Condition sharedCondition) {
        if (aggregate == null) {
            throw new IllegalArgumentException("aggregate should not be null");
        }
        if (aggregate.function == null) {
            throw new IllegalArgumentException("aggregate.function should not be null");
        }
        if (aggregate.buckets == null || aggregate.buckets.isEmpty()) {
            throw new IllegalArgumentException("aggregate.buckets should not be empty");
        }
        if (aggregate.buckets.size() > MAX_BUCKETS) {
            throw new QueryTooComplexException("Bucket count %d exceeds maximum %d"
                    .formatted(aggregate.buckets.size(), MAX_BUCKETS));
        }
        if (aggregate.function != BucketAggregateFunction.COUNT) {
            normalizeField(aggregate.field);
        }

        validateCondition(sharedCondition, "sharedCondition");

        var bucketIds = new HashSet<String>();
        for (var i = 0; i < aggregate.buckets.size(); i++) {
            var bucket = aggregate.buckets.get(i);
            if (bucket == null) {
                throw new IllegalArgumentException("aggregate.buckets[%d] should not be null".formatted(i));
            }
            if (StringUtils.isBlank(bucket.bucketId)) {
                throw new IllegalArgumentException("aggregate.buckets[%d].bucketId should be non-blank".formatted(i));
            }
            if (!bucketIds.add(bucket.bucketId)) {
                throw new IllegalArgumentException("Duplicate bucketId: " + bucket.bucketId);
            }
            if (bucket.condition == null) {
                throw new IllegalArgumentException("aggregate.buckets[%d].condition should not be null".formatted(i));
            }
            validateCondition(bucket.condition, "aggregate.buckets[%d].condition".formatted(i));
        }
    }

    /**
     * Convert supported Cosmos-style field notation to a portable dot path and validate it.
     */
    public static String normalizeField(String field) {
        if (StringUtils.isBlank(field)) {
            throw new IllegalArgumentException("aggregate.field should be non-blank for non-COUNT functions");
        }

        var normalized = FieldNameUtil.convertToDotFieldName(field.trim());
        if (StringUtils.isBlank(normalized)
                || StringUtils.containsAny(normalized, "\"", "'", "\\", "[", "]", "{", "}", ",", ";", "\n", "\r", "\t")) {
            throw new IllegalArgumentException("Invalid aggregate field: " + field);
        }
        for (var part : normalized.split("\\.", -1)) {
            if (StringUtils.isBlank(part) || part.startsWith("$")) {
                throw new IllegalArgumentException("Invalid aggregate field: " + field);
            }
        }
        return normalized;
    }

    /**
     * Enforce limits against the compiled request before it is sent to a database.
     */
    public static void checkCompiledRequest(String queryText, List<CosmosSqlParameter> params) {
        var safeParams = params == null ? List.<CosmosSqlParameter>of() : params;
        if (safeParams.size() > MAX_COMPILED_PARAMS) {
            throw new QueryTooComplexException("Compiled parameter count %d exceeds maximum %d"
                    .formatted(safeParams.size(), MAX_COMPILED_PARAMS));
        }

        long requestBytes = queryText == null ? 0 : queryText.getBytes(StandardCharsets.UTF_8).length;
        for (var param : safeParams) {
            requestBytes += JsonUtil.toJson(param == null ? null : param.value)
                    .getBytes(StandardCharsets.UTF_8).length;
            if (requestBytes > MAX_REQUEST_BYTES) {
                break;
            }
        }
        if (requestBytes > MAX_REQUEST_BYTES) {
            throw new QueryTooComplexException("Compiled request size %d bytes exceeds maximum %d bytes"
                    .formatted(requestBytes, MAX_REQUEST_BYTES));
        }
    }

    private static void validateCondition(Condition condition, String path) {
        if (condition == null) {
            return;
        }
        if (condition.rawQuerySpec != null) {
            throw new IllegalArgumentException(path + " does not support raw SQL");
        }
        if (condition.join != null && !condition.join.isEmpty()) {
            throw new IllegalArgumentException(path + " does not support joins");
        }
        if (condition.filter == null) {
            throw new IllegalArgumentException(path + ".filter should not be null");
        }

        for (var entry : condition.filter.entrySet()) {
            validateFilterEntry(entry, path);
        }
    }

    private static void validateFilterEntry(Map.Entry<String, Object> entry, String path) {
        var key = entry.getKey();
        var value = entry.getValue();
        if (StringUtils.isBlank(key)) {
            throw new IllegalArgumentException(path + " contains a blank filter key");
        }

        if (isSubCondition(key, SubConditionType.AND)
                || isSubCondition(key, SubConditionType.OR)
                || isSubCondition(key, SubConditionType.NOT)) {
            var children = Condition.extractSubQueries(value);
            if (children.isEmpty()) {
                throw new IllegalArgumentException(path + " contains an empty or invalid sub-condition: " + key);
            }
            for (var i = 0; i < children.size(); i++) {
                validateCondition(children.get(i), path + "." + key + "[" + i + "]");
            }
            return;
        }

        if (key.startsWith("$")) {
            throw new IllegalArgumentException(path + " contains an unsupported condition: " + key);
        }

        var simpleMatcher = Condition.simpleExpressionPattern.matcher(key);
        if (simpleMatcher.find()) {
            var field = simpleMatcher.group(1).trim();
            var operator = simpleMatcher.group(2).trim();
            validateConditionField(field, path);
            if (!ALLOWED_OPERATORS.contains(operator)) {
                throw new IllegalArgumentException(path + " contains an unsupported operator: " + operator);
            }
            validateCollectionOperator(operator, value, path);
            return;
        }

        var subQueryMatcher = Condition.subQueryExpressionPattern.matcher(key);
        if (subQueryMatcher.find()) {
            var joinField = subQueryMatcher.group(1).trim();
            var filterField = subQueryMatcher.group(3).trim();
            var operator = subQueryMatcher.group(2).trim();
            validateConditionField(joinField, path);
            if (StringUtils.isNotBlank(filterField)) {
                validateConditionField(filterField, path);
            }
            if (!ALLOWED_OPERATORS.contains(operator)) {
                throw new IllegalArgumentException(path + " contains an unsupported operator: " + operator);
            }
            validateCollectionOperator(operator, value, path);
            return;
        }

        validateConditionField(key.trim(), path);
        if (value instanceof Collection<?> collection && collection.isEmpty()) {
            throw new IllegalArgumentException(path + " contains an empty implicit array condition: " + key);
        }
    }

    private static boolean isSubCondition(String key, String operator) {
        return key.equals(operator) || key.startsWith(operator + " ");
    }

    private static void validateCollectionOperator(String operator, Object value, String path) {
        if (("ARRAY_CONTAINS_ANY".equals(operator) || "ARRAY_CONTAINS_ALL".equals(operator))
                && value instanceof Collection<?> collection && collection.isEmpty()) {
            throw new IllegalArgumentException(path + " contains an empty " + operator + " condition");
        }
        if ("IN".equals(operator) && (!(value instanceof Collection<?>) || ((Collection<?>) value).isEmpty())) {
            throw new IllegalArgumentException(path + " requires a non-empty collection for IN");
        }
    }

    private static void validateConditionField(String field, String path) {
        if (StringUtils.isBlank(field)
                || StringUtils.containsAny(field, "\"", "'", "\\", "[", "]", "{", "}", ",", ";", "\n", "\r", "\t")) {
            throw new IllegalArgumentException(path + " contains an invalid field: " + field);
        }
    }
}
