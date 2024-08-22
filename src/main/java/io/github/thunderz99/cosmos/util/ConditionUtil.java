package io.github.thunderz99.cosmos.util;

import java.util.*;
import java.util.regex.Pattern;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.FieldKey;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * An util class to convert condition's filter/sort/limit/offset to bson filter/sort/limit/offset for mongo
 */
public class ConditionUtil {

    static final List<String> binaryOperators = List.of(
            "LIKE", "IN", "=", "!=", "<", "<=", ">", ">=",
            "STARTSWITH", "ENDSWITH", "CONTAINS", "RegexMatch",
            "ARRAY_CONTAINS", "ARRAY_CONTAINS_ANY", "ARRAY_CONTAINS_ALL"
    );

    static final Map<String, String> OPERATOR_MAPPINGS = Map.of(
            "=", "$eq",
            "!=", "$ne",
            ">=", "$gte",
            "<=", "$lte",
            ">", "$gt",
            "<", "$lt"
    );

    // Generate the regex pattern using binaryOperators
    public static final Pattern simpleExpressionPattern = Pattern.compile("(.+?)\\s*(" + String.join("|", binaryOperators) + ")\\s*$");
    ;

    /**
     * Convert condition's map filter to bson filter for mongo
     *
     * @param map
     * @return bson filter
     */
    public static Bson toBsonFilter(Map<String, Object> map) {
        if (MapUtils.isEmpty(map)) {
            return new BsonDocument();
        }

        var filters = new ArrayList<Bson>();

        for (var entry : map.entrySet()) {
            var key = entry.getKey();

            var value = entry.getValue();

            if (StringUtils.isEmpty(key)) {
                // Ignore when key is empty
                continue;
            }

            var matcher = simpleExpressionPattern.matcher(key);
            if (matcher.matches()) {
                var field = matcher.group(1).trim();
                var operator = matcher.group(2).trim();

                switch (operator) {
                    case "=":
                    case "!=":
                    case ">=":
                    case "<=":
                    case ">":
                    case "<":
                        filters.add(generateExpression(field, operator, value));
                        break;
                    case "LIKE":
                        // Convert SQL-like wildcards to MongoDB regex equivalents
                        String regexValue = value.toString()
                                .replace("%", ".*")  // % to match any number of characters
                                .replace("_", ".");  // _ to match exactly one character
                        filters.add(Filters.regex(field, regexValue));
                        break;
                    case "STARTSWITH":
                        filters.add(Filters.regex(field, "^" + Pattern.quote(value.toString())));
                        break;
                    case "ENDSWITH":
                        filters.add(Filters.regex(field, Pattern.quote(value.toString()) + "$"));
                        break;
                    case "CONTAINS":
                        filters.add(Filters.regex(field, ".*" + Pattern.quote(value.toString()) + ".*"));
                        break;
                    case "RegexMatch":
                        filters.add(Filters.regex(field, value.toString()));
                        break;
                    case "ARRAY_CONTAINS":
                        // eq does the job
                        // https://www.mongodb.com/docs/manual/tutorial/query-arrays/?msockid=07d12f08b23369f53c0f3b60b31168fe#query-an-array-for-an-element
                        filters.add(Filters.eq(field, value));
                        break;
                    case "ARRAY_CONTAINS_ANY":
                        filters.add(Filters.in(field, (Collection<?>) value));
                        break;
                    case "ARRAY_CONTAINS_ALL":
                        filters.add(Filters.all(field, (Collection<?>) value));
                        break;
                    case "IN":
                        filters.add(Filters.in(field, (Collection<?>) value));
                        break;
                    default:
                        break;
                }
            } else if (key.startsWith("$OR")) {
                filters.add(Filters.or(toBsonFilters((List<Map<String, Object>>) value)));
            } else if (key.startsWith("$AND")) {
                filters.add(Filters.and(toBsonFilters((List<Map<String, Object>>) value)));
            } else if (key.startsWith("$NOT")) {
                if (value instanceof Collection<?>) {
                    filters.add(Filters.not(Filters.and(toBsonFilters((Collection<?>) value))));
                } else if (value instanceof Condition) {
                    filters.add(Filters.not(toBsonFilter((Condition) value)));
                } else if (value instanceof Map<?, ?>) {
                    filters.add(Filters.not(toBsonFilter((Map<String, Object>) value)));
                } else {
                    throw new IllegalArgumentException("$NOT 's filter is not correct. expect Collection/Map/Condition:" + value);
                }
            } else {
                if (value instanceof Collection<?>) {
                    // the same as IN
                    var coll = (Collection<?>) value;
                    filters.add(Filters.in(key, coll));
                } else {
                    // normal eq filter. and support $fieldA = $fieldB case
                    filters.add(generateExpression(key, "=", value));
                }
            }
        }

        return filters.size() == 1 ? filters.get(0) : Filters.and(filters);
    }

    /**
     * Generate bson filter from field, operator and value. Supports "$fieldA != $fieldB"
     *
     * @param field
     * @param operator
     * @param value
     * @return bson filter
     */
    static Bson generateExpression(String field, String operator, Object value) {

        Bson ret = null;

        if (value instanceof FieldKey) {
            // support $fieldA != $fieldB
            var processField = "$" + field;
            var processedOperator = OPERATOR_MAPPINGS.get(operator);
            var processedValue = "$" + ((FieldKey) value).keyName;

            /*
            $expr is required for "$fieldA != $fieldB" case
            $expr: {
                $ne: ["$user.fieldA", "$user.fieldB"]
            }
            */
            ret = Filters.expr(new Document(processedOperator, List.of(processField, processedValue)));
        } else {
            // normal simple queries
            switch (operator) {
                case "=":
                    ret = Filters.eq(field, value);
                    break;
                case "!=":
                    ret = Filters.ne(field, value);
                    break;
                case ">=":
                    ret = Filters.gte(field, value);
                    break;
                case "<=":
                    ret = Filters.lte(field, value);
                    break;
                case ">":
                    ret = Filters.gt(field, value);
                    break;
                case "<":
                    ret = Filters.lt(field, value);
                    break;
            }
        }
        return ret;
    }

    /**
     * convert list of maps for nested queries
     *
     * @param subFilters
     * @return bson filters
     */
    static List<Bson> toBsonFilters(Collection<?> subFilters) {
        List<Bson> bsonFilters = new ArrayList<>();
        for (var filter : subFilters) {
            if (filter instanceof Condition) {
                bsonFilters.add(toBsonFilter((Condition) filter));
            } else if (filter instanceof Map<?, ?>) {
                bsonFilters.add(toBsonFilter((Map<String, Object>) filter));
            } else {
                throw new IllegalArgumentException("subFilters' type is not valid. expect Condition or Map<String, Object: " + subFilters);
            }
        }
        return bsonFilters;
    }

    /**
     * Convert cond obj to bson filter for mongo
     *
     * @param cond
     * @return bson filter
     */
    public static Bson toBsonFilter(Condition cond) {
        if (!cond.negative) {
            // a normal filter
            return toBsonFilter(cond.filter);
        } else {
            // process a NOT filter
            return Filters.not(toBsonFilter(cond.filter));
        }
    }

    /**
     * Convert List.of("id", "DESC") or List.of("_ts", "ASC") to bson sort for mongo
     * <pre>
     *     ["age"] means ["age", "ASC"]
     * </pre>
     *
     * @param sort
     * @return bson sort
     */
    public static Bson toBsonSort(List<String> sort) {
        if (sort == null || sort.isEmpty()) {
            return new org.bson.BsonDocument(); // Empty sort
        }

        List<Bson> bsonSortList = new ArrayList<>();

        for (int i = 0; i < sort.size(); i += 2) {
            var field = sort.get(i);
            var order = (i + 1 < sort.size()) ? sort.get(i + 1).toUpperCase() : "ASC";

            switch (order) {
                case "DESC":
                    bsonSortList.add(Sorts.descending(field));
                    break;
                case "ASC":
                default:
                    bsonSortList.add(Sorts.ascending(field));
                    break;
            }
        }

        return Sorts.orderBy(bsonSortList);
    }

    /**
     * Convert $not to $nor because MongoDB does not support a top-level $not operator
     *
     * @param filter the BSON filter to process
     * @return the filter processed with $nor if it originally contained a top-level $not
     */
    public static Bson processNor(Bson filter) {
        if (filter == null) {
            return filter;
        }

        BsonDocument bsonDoc = filter.toBsonDocument();
        if (bsonDoc.isEmpty()) {
            return filter;
        }

        // Check if the filter is a document and if it has a top-level $not operator
        if (!bsonDoc.isDocument() || !StringUtils.equals("$not", bsonDoc.getFirstKey())) {
            // Not a top-level $not, return the original filter
            return filter;
        }

        // Begin processing the top-level $not
        var subFilter = bsonDoc.get(bsonDoc.getFirstKey());

        if (subFilter.isArray()) {
            // If the sub-filter is an array, apply $nor on each element
            return Filters.nor(subFilter.asArray().stream()
                    .map(BsonValue::asDocument)
                    .toArray(Bson[]::new));
        } else if (subFilter.isDocument()) {
            // If the sub-filter is a document, apply $nor on that document
            return Filters.nor(subFilter.asDocument());
        } else {
            // Unexpected case, simply return the original filter
            return filter;
        }
    }

    /**
     * check and generate fields for mongo find
     *
     * @param fields
     * @return field list
     */
    public static List<String> processFields(Set<String> fields) {
        if (CollectionUtils.isEmpty(fields)) {
            return List.of();
        }

        var ret = new ArrayList<String>();

        for (var field : fields) {
            if (StringUtils.containsAny(field, "{", "}", ",", "\"", "'")) {
                throw new IllegalArgumentException("field cannot contain '{', '}', ',', '\"', \"'\", field: " + field);
            }
            ret.add(field);
        }
        return ret;
    }
}

