package io.github.thunderz99.cosmos.util;

import java.util.*;
import java.util.regex.Pattern;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.FieldKey;
import io.github.thunderz99.cosmos.dto.FilterOptions;
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
            "ARRAY_CONTAINS", "ARRAY_CONTAINS_ANY", "ARRAY_CONTAINS_ALL",
            "IS_DEFINED", "IS_NULL", "IS_NUMBER" // TODO IS_ARRAY, IS_STRING, etc
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

    /**
     * Convert condition's map filter to bson filter for mongo
     *
     * @param map
     * @return bson filter
     */
    public static Bson toBsonFilter(Map<String, Object> map) {
        return toBsonFilter(map, FilterOptions.create());
    }
    /**
     * Convert condition's map filter to bson filter for mongo
     *
     * @param map
     * @param filterOptions
     * @return bson filter
     */
    public static Bson toBsonFilter(Map<String, Object> map, FilterOptions filterOptions) {
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
            filters.add(toBsonFilter(key, value, filterOptions));

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
        return generateExpression(field, operator, value, FilterOptions.create());
    }

    /**
     * Generate bson filter from field, operator and value. Supports "$fieldA != $fieldB"
     *
     * @param field
     * @param operator
     * @param value
     * @param filterOptions whether this filter is used in an innerCond. Or if this is used in join. In innerCond $eq is a must for(key = value)
     * @return bson filter
     */
    static Bson generateExpression(String field, String operator, Object value, FilterOptions filterOptions) {

        Bson ret = null;

        var innerCond = filterOptions.innerCond;

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
                    ret = innerCond ? new Document("$eq", List.of(field, value)) : Filters.eq(field, value);
                    break;
                case "!=":
                    ret = innerCond ? new Document("$ne", List.of(field, value)) : Filters.ne(field, value);
                    break;
                case ">=":
                    ret = innerCond ? new Document("$gte", List.of(field, value)) : Filters.gte(field, value);
                    break;
                case "<=":
                    ret = innerCond ? new Document("$lte", List.of(field, value)) : Filters.lte(field, value);
                    break;
                case ">":
                    ret = innerCond ? new Document("$gt", List.of(field, value)) : Filters.gt(field, value);
                    break;
                case "<":
                    ret = innerCond ? new Document("$lt", List.of(field, value)) : Filters.lt(field, value);
                    break;
            }
        }
        return ret;
    }

    /**
     * Generate a single bson filter by key / value. return null if invalid
     *
     * @param key
     * @param value
     * @param filterOptions
     * @return single bson filter
     */
    public static Bson toBsonFilter(String key, Object value, FilterOptions filterOptions) {

        Bson ret = null;

        if (StringUtils.isEmpty(key)) {
            // Ignore when key is empty
            return null;
        }

        // preprocess for JOIN
        // convert "area.city.street.rooms.floor" to "floor" to get ready for "$elemMatch";
        var originKey = key;
        var joinKey = filterOptions.join.stream().filter( joinPart -> StringUtils.startsWith(originKey, joinPart)).findFirst();

        var elemMatch = false;
        if(joinKey.isPresent()){
            key = StringUtils.removeStart(key, joinKey.get() + ".");
            elemMatch = true;
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
                    ret = generateExpression(field, operator, value, filterOptions);
                    break;
                case "LIKE":
                    // Convert SQL-like wildcards to MongoDB regex equivalents
                    String regexValue = value.toString()
                            .replace("%", ".*")  // % to match any number of characters
                            .replace("_", ".");  // _ to match exactly one character
                    ret = Filters.regex(field, regexValue);
                    break;
                case "STARTSWITH":
                    ret = Filters.regex(field, "^" + Pattern.quote(value.toString()));
                    break;
                case "ENDSWITH":
                    ret = Filters.regex(field, Pattern.quote(value.toString()) + "$");
                    break;
                case "CONTAINS":
                    ret = Filters.regex(field, ".*" + Pattern.quote(value.toString()) + ".*");
                    break;
                case "RegexMatch":
                    ret = Filters.regex(field, value.toString());
                    break;
                case "IS_DEFINED":
                    ret = Filters.exists(field, Boolean.valueOf(value.toString()));
                    break;
                case "IS_NULL":
                    if (Boolean.valueOf(value.toString())) {
                        // IS_NULL = true
                        // IS_NULL means "field exists" AND "value is null"
                        ret = Filters.and(Filters.exists(field, true), Filters.eq(field, null));
                    } else {
                        // IS_NULL = false
                        // "IS_NULL= false" means "field not exists" OR "value not null"
                        ret = Filters.or(Filters.exists(field, false), Filters.ne(field, null));
                    }
                    break;
                case "IS_NUMBER":
                    if (Boolean.valueOf(value.toString())) {
                        // IS_NUMBER = true
                        ret = Filters.type(field, "number");
                    } else {
                        // IS_NUMBER = false
                        ret = Filters.not(Filters.type(field, "number"));
                    }
                    break;
                case "ARRAY_CONTAINS":
                    // eq does the job
                    // https://www.mongodb.com/docs/manual/tutorial/query-arrays/?msockid=07d12f08b23369f53c0f3b60b31168fe#query-an-array-for-an-element
                    ret = Filters.eq(field, value);
                    break;
                case "ARRAY_CONTAINS_ANY":
                    if(value instanceof Collection){
                        ret = Filters.in(field, (Collection<?>) value);
                    } else {
                        ret = Filters.in(field, List.of(value));
                    }
                    break;
                case "ARRAY_CONTAINS_ALL":
                    ret = Filters.all(field, (Collection<?>) value);
                    break;
                case "IN":
                    ret = Filters.in(field, (Collection<?>) value);
                    break;
                default:
                    break;
            }
        } else if (key.startsWith("$OR")) {
            if (value instanceof Collection<?>) {
                ret = Filters.or(toBsonFilters((Collection<?>) value, filterOptions));
            } else if (value instanceof Condition) {
                ret = Filters.or(toBsonFilters(List.of(value), filterOptions));
            } else if (value instanceof Map<?, ?>) {
                ret = Filters.or(toBsonFilters(List.of(value), filterOptions));
            } else {
                throw new IllegalArgumentException("$OR 's filter is not correct. expect Collection/Map/Condition:" + value);
            }
        } else if (key.startsWith("$AND")) {
            if (value instanceof Collection<?>) {
                ret = Filters.and(toBsonFilters((Collection<?>) value, filterOptions));
            } else if (value instanceof Condition) {
                ret = Filters.and(toBsonFilters(List.of(value), filterOptions));
            } else if (value instanceof Map<?, ?>) {
                ret = Filters.and(toBsonFilters(List.of(value), filterOptions));
            } else {
                throw new IllegalArgumentException("$AND 's filter is not correct. expect Collection/Map/Condition:" + value);
            }

        } else if (key.startsWith("$NOT")) {
            if (value instanceof Collection<?>) {
                ret = Filters.not(Filters.and(toBsonFilters((Collection<?>) value, filterOptions)));
            } else if (value instanceof Condition) {
                ret = Filters.not(toBsonFilter((Condition) value, filterOptions));
            } else if (value instanceof Map<?, ?>) {
                ret = Filters.not(toBsonFilter((Map<String, Object>) value, filterOptions));
            } else {
                throw new IllegalArgumentException("$NOT 's filter is not correct. expect Collection/Map/Condition:" + value);
            }
        } else {
            if (value instanceof Collection<?>) {
                // the same as IN
                var coll = (Collection<?>) value;
                ret = Filters.in(key, coll);
            } else {
                // normal eq filter. and support $fieldA = $fieldB case
                ret = generateExpression(key, "=", value, filterOptions);
            }
        }

        // finally add "$elemMatch" for JOIN
        if(elemMatch && ret != null){
            ret = Filters.elemMatch(joinKey.get(), ret);
        }

        return ret;
    }


    /**
     * convert list of maps for nested queries
     *
     * @param subFilters
     * @return bson filters
     */
    static List<Bson> toBsonFilters(Collection<?> subFilters, FilterOptions filterOptions) {
        List<Bson> bsonFilters = new ArrayList<>();
        for (var filter : subFilters) {
            if (filter instanceof Condition) {
                bsonFilters.add(toBsonFilter((Condition) filter, filterOptions));
            } else if (filter instanceof Map<?, ?>) {
                bsonFilters.add(toBsonFilter((Map<String, Object>) filter, filterOptions));
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
        if (cond == null) {
            return null;
        }

        var filterOptions = FilterOptions.create().join(cond.join);

        if (!cond.negative) {
            // a normal filter
            return toBsonFilter(cond.filter, filterOptions);
        } else {
            // process a NOT filter
            return Filters.not(toBsonFilter(cond.filter, filterOptions));
        }
    }

    /**
     * Convert cond obj to bson filter for mongo
     *
     * @param cond
     * @param filterOptions
     * @return bson filter
     */
    public static Bson toBsonFilter(Condition cond, FilterOptions filterOptions) {
        if (cond == null) {
            return null;
        }

        if (!cond.negative) {
            // a normal filter
            return toBsonFilter(cond.filter, filterOptions);
        } else {
            // process a NOT filter
            return Filters.not(toBsonFilter(cond.filter, filterOptions));
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
            return null; // Empty sort
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
            return null;
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

    /**
     * Generate a nested structure for $mergeObjects
     *
     * @param fieldName
     * @param replaceFieldName
     * @return the result bson representing the nested parts
     */
    public static Bson generateMergeObjects(String fieldName, String replaceFieldName) {

        var fields = List.of(fieldName.split("\\.")); // Split fieldName into its parts

        // Start with the replace field
        var current = new Document(fields.get(fields.size() - 1), "$" + replaceFieldName);

        // Build the nested structure from the bottom up
        for (int i = fields.size() - 2; i >= 0; i--) {
            current = new Document(fields.get(i), current);
        }
        return current;
    }
}

