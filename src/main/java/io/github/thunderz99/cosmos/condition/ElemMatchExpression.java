package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

/**
 * A class representing ElemMatch expression in WHERE EXIST style, which is used in Condition.join query
 * <p>
 * {@code
 * // ElemMatch expression for join, using WHERE EXIST style, will AND multiple sub conditions
 * // Condition.filter("$ELEM_MATCH", Map.of("rooms.no", "001", "rooms.name", "room-01"))
 * // EXISTS (
 * SELECT VALUE rooms FROM rooms in c["rooms"]
 * WHERE (rooms.no = "001") AND (rooms.name = "room-01")
 * )
 * //
 * }
 */
public class ElemMatchExpression implements Expression {

    private final Map<String, Object> subFilters;
    private final Condition condition;

    /**
     * Constructs an ElemMatchExpression.
     *
     * @param subFilters A map where keys are field paths within the array elements (e.g., "arrayField.subField")
     *                   and values are the conditions for those fields.
     * @param condition  The parent Condition object.
     */
    public ElemMatchExpression(Map<String, Object> subFilters, Condition condition) {
        this.subFilters = subFilters;
        this.condition = condition;
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {
        // Determine the array field name from the subFilters keys.
        // Expects keys like "arrayField.subField".
        var arrayField = subFilters.keySet().stream().findFirst()
                .map(k -> StringUtils.substringBefore(k, ".")).orElse("");

        if (StringUtils.isEmpty(arrayField)) {
            throw new IllegalArgumentException("$ELEM_MATCH requires a field path like 'arrayField.subField'");
        }

        var params = new ArrayList<CosmosSqlParameter>();
        var subCondTexts = new ArrayList<String>();

        // Iterate through each sub-filter to build individual conditions for array elements.
        for (var subEntry : subFilters.entrySet()) {
            var key = subEntry.getKey();
            // Ensure consistency: all sub-filters must refer to the same array field.
            if (!key.startsWith(arrayField + ".")) {
                throw new IllegalArgumentException(String.format(
                        "Inconsistent array field in $ELEM_MATCH. Expected prefix '%s.' but found key '%s'",
                        arrayField, key));
            }
            // Parse each sub-filter into an Expression and convert it to a query spec.
            var exp = Condition.parse(key, subEntry.getValue());
            var spec = exp.toQuerySpec(paramIndex, selectAlias);
            params.addAll(spec.getParameters());
            subCondTexts.add(spec.getQueryText());
        }

        // Combine all sub-condition texts with " AND " to form the WHERE clause for the array elements.
        var combinedCondText = String.join(" AND ", subCondTexts);
        // Save the original join condition for potential later use (e.g., in nested queries).
        condition.saveOriginJoinCondition(combinedCondText);

        // Generate the final query text using the parent condition's utility method,
        var queryText = condition.toJoinQueryText(arrayField, combinedCondText, paramIndex);
        return new CosmosSqlQuerySpec(queryText, params);
    }

    /**
     * Returns a JSON string representation of this ElemMatchExpression.
     *
     * @return JSON string.
     */
    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }
}