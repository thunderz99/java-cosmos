package io.github.thunderz99.cosmos.util;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import static io.github.thunderz99.cosmos.util.ConditionUtil.OPERATOR_MAPPINGS;


/**
 * Convert SQL expression filter to mongodb bson filter
 *
 * <pre>
 *     input: "c.currentPhaseIndex < ARRAY_LENGTH(c.phases) - 1"
 *     output: { "currentPhaseIndex": { "$lt": { "$subtract": [ { "$size": "$phases" }, 1 ] } } }
 *
 * </pre>
 */
public class ExpressionConvertUtil {

    /**
     * A registry for predefined functions and their mappings to MongoDB operators
     */
    static final Map<String, Function<String, Map<String, Object>>> FUNCTION_REGISTRY = Map.of(
            "ARRAY_LENGTH", field -> Map.of("$size", Map.of("$ifNull", List.of("$" + extractFieldName(field), List.of()))),
            "MIN", field -> Map.of("$min", "$" + extractFieldName(field)),
            "MAX", field -> Map.of("$max", "$" + extractFieldName(field))
            // Add more functions here as needed, like SUM, AVG, etc.
    );

    /**
     * Entry point method to convert Azure Cosmos DB SQL to MongoDB filter query
     *
     * @param expression
     * @return mongo filter map
     */
    public static Map<String, Object> convert(String expression) {
        return parseComparisonExpression(expression);
    }

    /**
     * Parsing the expression recursively
     * @param expression
     * @return
     */
    static Map<String, Object> parseComparisonExpression(String expression) {
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
    static Map<String, Object> buildComparisonQuery(String leftOperand, String operator, String rightOperand) {
        var mongoOperator = getMongoComparisonOperator(operator);
        if (mongoOperator == null) {
            throw new IllegalArgumentException("Unsupported comparison operator: " + operator);
        }

        // Parse left operand (could be a literal number or a field reference)
        var leftValue = getValueFromExpression(leftOperand);
        // Parse right operand (could be a literal number or a field reference)
        var rightValue = getValueFromExpression(rightOperand);

        return Map.of("$expr",
                Map.of(mongoOperator, List.of(leftValue, rightValue)));
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
                return Map.of(getMongoOperator(operator), List.of(left, right));
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
            value = "$" + extractFieldName(expression);
        } else {
            throw new IllegalArgumentException("Not supported expression:" + expression);
        }
        return value;
    }

    /**
     * Helper method to get MongoDB comparison operator equivalent
     * @param operator
     * @return comparison operator in mongo
     */
    static String getMongoComparisonOperator(String operator) {
        var ret = OPERATOR_MAPPINGS.get(operator);
        if(StringUtils.isEmpty(ret)){
            throw new IllegalArgumentException("Unknown operator: " + operator + ". Supported:" + OPERATOR_MAPPINGS.keySet());
        }
        return ret;
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
     * Helper method to get MongoDB arithmetic operator
     * @param operator op in azure cosmos
     * @return operator in mongo
     */
    static String getMongoOperator(String operator) {
        switch (operator) {
            case "+": return "$add";
            case "-": return "$subtract";
            case "*": return "$multiply";
            case "/": return "$divide";
            case "%": return "$mod";
            default: throw new IllegalArgumentException("Unknown operator: " + operator + ". Only support + - * / %");
        }
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

