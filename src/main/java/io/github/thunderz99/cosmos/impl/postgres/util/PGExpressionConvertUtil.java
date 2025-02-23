package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.util.FieldNameUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Convert SQL expression filter to mongodb bson filter
 *
 * <p>
 * {@code
 * // input: "c.currentPhaseIndex < ARRAY_LENGTH(c.phases) - 1"
 * // output: (data->>'currentPhaseIndex')::int < jsonb_array_length(data->>'phases')
 * }
 * </p>
 */
public class PGExpressionConvertUtil {

    /**
     * A registry for predefined functions and their mappings to MongoDB operators
     */
    static final Map<String, Function<String, String>> FUNCTION_REGISTRY = Map.of(
            "ARRAY_LENGTH", field -> "jsonb_array_length(%s)".formatted(PGKeyUtil.getFormattedKey4JsonWithAlias(extractFieldName(field), TableUtil.DATA)),
            "MIN", field -> "min(%s)".formatted(PGKeyUtil.getFormattedKeyWithAlias(extractFieldName(field), TableUtil.DATA, 0)),
            "MAX", field -> "max(%s)".formatted(PGKeyUtil.getFormattedKeyWithAlias(extractFieldName(field), TableUtil.DATA, 0))
            // Add more functions here as needed, like SUM, AVG, etc.
    );

    /**
     * Entry point method to convert Azure Cosmos DB SQL to MongoDB filter query
     *
     * @param expression
     * @return mongo filter map
     */
    public static String convert(String expression) {
        return parseComparisonExpression(expression);
    }

    /**
     * Parsing the expression recursively
     * @param expression
     * @return
     */
    static String parseComparisonExpression(String expression) {
        expression = expression.trim();

        // Handle parentheses recursively
        if (expression.startsWith("(") && expression.endsWith(")")) {
            return parseComparisonExpression(expression.substring(1, expression.length() - 1));
        }

        // Handle logical operators (AND, OR)
        if (StringUtils.containsAny(" AND ", " OR ", " NOT ")) {
            throw new IllegalArgumentException("$EXPRESSION do not support AND / OR / NOT");
        }

        // Handle comparison operators: <, <=, >, >=, =, !=
        String[] comparisonOperators = {"<=", ">=", "<", ">", "!=", "="};
        for (var operator : comparisonOperators) {
            var index = expression.indexOf(operator);
            if (index != -1) {
                var leftOperand = expression.substring(0, index).trim();
                var rightOperand = expression.substring(index + operator.length()).trim();
                return buildComparisonQuery(leftOperand, operator, rightOperand);
            }
        }

        throw new IllegalArgumentException("$EXPRESSION should contain <, <=, >, >=, =, !=");
    }

    /**
     * Helper method to build a comparison query for MongoDB
     * @param leftOperand
     * @param operator
     * @param rightOperand
     * @return mongo query map
     */
    static String buildComparisonQuery(String leftOperand, String operator, String rightOperand) {

        // Parse left operand (could be a literal number or a field reference)
        var leftValue = getValueFromExpression(leftOperand);
        // Parse right operand (could be a literal number or a field reference)
        var rightValue = getValueFromExpression(rightOperand);

        return " (%s %s %s)".formatted(leftValue, operator, rightValue);
    }

    /**
     * get the mongo value used in expression
     * @param expression
     * @return mongo value used in expression
     */
    static Object getValueFromExpression(String expression) {

        expression = expression.trim();

        // Handle parentheses recursively
        if (expression.startsWith("(") && expression.endsWith(")")) {
            return getValueFromExpression(expression.substring(1, expression.length() - 1));
        }

        // Check for arithmetic operators and split the expression
        for (var operator : List.of("+", "-", "*", "/", "%")) {
            var index = findOperatorIndex(expression, operator);
            if (index != -1) {
                var left = getValueFromExpression(expression.substring(0, index));
                var right = getValueFromExpression(expression.substring(index + 1));
                return "(%s %s %s)".formatted(left, operator, right);
            }
        }

        // Handle predefined functions like ARRAY_LENGTH, MIN, MAX
        for (var functionName : FUNCTION_REGISTRY.keySet()) {
            var pattern = Pattern.compile(functionName + "\\(([^\\)]+)\\)");
            var matcher = pattern.matcher(expression);
            if (matcher.find()) {
                var field = matcher.group(1).trim();
                return FUNCTION_REGISTRY.get(functionName).apply(field);
            }
        }

        // simple values
        Object value = null;
        if(StringUtils.isEmpty(expression)){
            value = "";
        } else if(NumberUtils.isCreatable(expression)){
            value = NumberUtils.createNumber(expression)  ;
        } else if(StringUtils.startsWithAny(expression,"c.", "c[")){
            // simple field
            // assume all the calculation is for int type
            // TODO: deal with other types
            value = PGKeyUtil.getFormattedKeyWithAlias(extractFieldName(expression), TableUtil.DATA, 0);
        } else {
            throw new IllegalArgumentException("Not supported expression:" + expression);
        }
        return value;
    }

    /**
     * Helper method to extract the field from "c.xxx.yyy" / c['address-detail']['city'] / c["children"] format
     * <pre>
     *     output: xxx.yyy / address-detail.city / children
     * </pre>
     * @param field
     * @return field in mongo format
     */
    static String extractFieldName(String field) {
        return FieldNameUtil.convertToDotFieldName(field);
    }

    /**
     * Helper method to find the index of the operator (ignores operators inside parentheses)
     * @param expression
     * @param operator
     * @return index
     */
    static int findOperatorIndex(String expression, String operator) {
        int depth = 0;
        for (int i = 0; i < expression.length(); i++) {
            char ch = expression.charAt(i);
            if (ch == '(') depth++;
            else if (ch == ')') depth--;
            else if (depth == 0 && expression.startsWith(operator, i)) {
                return i;
            }
        }
        return -1;
    }
}

