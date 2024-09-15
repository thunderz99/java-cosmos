package io.github.thunderz99.cosmos.util;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

import static io.github.thunderz99.cosmos.condition.SubConditionType.*;

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

        List<Bson> filters = new ArrayList<>();

        for (var entry : map.entrySet()) {
            var key = entry.getKey();

            var value = entry.getValue();

            if (StringUtils.isEmpty(key)) {
                // Ignore when key is empty
                continue;
            }
            filters.add(toBsonFilter(key, value, filterOptions));

        }

        // filter invalid bson (null)
        filters = filters.stream().filter(Objects::nonNull).collect(Collectors.toList());

        if (filters.isEmpty()) {
            return trueBsonFilter();
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

            value = processCustomClassValue(value);

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
     * convert some custom class to normal class that can be used in bson filter
     *
     * @param value
     * @return
     */
    static Object processCustomClassValue(Object value) {
        // use JsonPatchUtil's getNormalizedValue method can do this
        return JsonPatchUtil.getNormalizedValue(value);
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

        // preprocess for "fieldA OR fieldB". exclude $AND, $OR, $NOT sub queries
        if (key.contains(" OR ") && !StringUtils.startsWithAny(key, AND, OR, NOT, ELEM_MATCH, EXPRESSION)) {
            return generateOrExpression(key, value, filterOptions);
        }

        // preprocess for JOIN
        // convert "area.city.street.rooms.floor" to "floor" to get ready for "$elemMatch";
        var originKey = key;
        var joinKey = filterOptions.join.stream().filter(joinPart -> StringUtils.startsWith(originKey, joinPart)).findFirst();

        var elemMatch = false;
        if (joinKey.isPresent()) {
            key = StringUtils.removeStart(key, joinKey.get() + ".");
            elemMatch = true;
        }

        var matcher = simpleExpressionPattern.matcher(key);


        if (StringUtils.startsWithAny(key, AND, OR, NOT, ELEM_MATCH, EXPRESSION)) {
            // query with sub conditions
            ret = toBsonFilter4SubConditions(key, value, filterOptions);

        } else if (matcher.matches()) {
            // match expressions
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
                    if (filterOptions.innerCond) {
                        ret = new Document("$in", List.of(value, field));
                    } else {
                        // eq does the job
                        // https://www.mongodb.com/docs/manual/tutorial/query-arrays/?msockid=07d12f08b23369f53c0f3b60b31168fe#query-an-array-for-an-element
                        ret = Filters.eq(field, value);
                    }
                    break;
                case "ARRAY_CONTAINS_ANY":
                    Collection<?> collectionValue = null;
                    if (value instanceof Collection) {
                        collectionValue = (Collection<?>) value;
                    } else {
                        collectionValue = List.of(value);
                    }

                    if (filterOptions.innerCond) {
                        // under an inner cond for aggregate pipelines
                        // $in does not work for aggregation, we have to utilize $setIntersection
                        /* e.g.
                        $project: {
                          original: "$$ROOT",  // Include all original fields
                          // Filter the "rooms" array to include only those elements with a 'no' array containing 3 or 5
                          matchingRooms: {
                            $filter: {
                              input: "$rooms",
                              cond: {
                                $gt: [
                                  { $size: { $setIntersection: ["$$this.no", [3, 5]] } },
                                  0
                                ]  // Check if the intersection of the 'no' array and [3, 5] has more than 0 elements
                              }
                            }
                          }
                        }
                         */
                        ret = new Document("$gt", List.of(new Document("$size",
                                new Document("$setIntersection", List.of(field, collectionValue))), 0));
                    } else {
                        // normal query filter
                        ret = Filters.in(field, collectionValue);
                    }

                    break;
                case "ARRAY_CONTAINS_ALL":
                    Collection<?> collectionValueAll = null;
                    if (value instanceof Collection) {
                        collectionValueAll = (Collection<?>) value;
                    } else {
                        collectionValueAll = List.of(value);
                    }

                    if (filterOptions.innerCond) {
                        // under an inner cond for aggregate pipelines
                        // $in does not work for aggregation, we have to utilize $setIsSubset
                        /* e.g.
                        $project: {
                          original: "$$ROOT",  // Include all original fields
                          // Filter the "rooms" array to include only those elements with a 'no' array containing both 2 and 3
                          matchingRooms: {
                            $filter: {
                              input: "$rooms",
                              cond: {
                                $setIsSubset: [[2, 3], "$$this.no"]  // Check if [2, 3] is a subset of the 'no' array
                              }
                            }
                          }
                        }
                         */
                        ret = new Document("$setIsSubset", List.of(collectionValueAll, field));
                    } else {
                        // normal query filter
                        ret = Filters.all(field, collectionValueAll);
                    }
                    break;
                case "IN":
                    ret = Filters.in(field, (Collection<?>) value);
                    break;
                default:
                    break;
            }
        } else {
            // 
            // process "tags ARRAY_CONTAINS_ANY id" or "children ARRAY_CONTAINS_ALL grade"
            var subQueryMatcher = Condition.subQueryExpressionPattern.matcher(key);

            if (subQueryMatcher.find()) {
                var subQueryJoinKey = subQueryMatcher.group(1); // children
                var operator = subQueryMatcher.group(2); // ARRAY_CONTAINS_ANY
                var filterKey = subQueryMatcher.group(3); // grade
                ret = generateExpression4SubQuery(subQueryJoinKey, operator, filterKey, value, filterOptions);

            } else {
                // normal {"key=": value} pattern 
                if (value instanceof Collection<?>) {
                    // the same as IN
                    var coll = (Collection<?>) value;
                    ret = Filters.in(key, coll);
                } else {
                    // normal eq filter. and support $fieldA = $fieldB case
                    ret = generateExpression(key, "=", value, filterOptions);
                }
            }
        }

        // finally add "$elemMatch" for JOIN
        if (elemMatch && ret != null) {
            ret = Filters.elemMatch(joinKey.get(), ret);
        }

        return ret;
    }

    /**
     * Generate sub condition bson filter for $AND, $OR, $NOT
     *
     * @param key
     * @param value
     * @param filterOptions
     * @return bson filter
     */
    static Bson toBsonFilter4SubConditions(String key, Object value, FilterOptions filterOptions) {

        if (StringUtils.isEmpty(key) || value == null) {
            // invalid key / value for sub conditions
            return null;
        }

        if (value instanceof Collection<?>) {
            // empty sub conditions
            if (CollectionUtils.isEmpty((Collection<?>) value)) {
                return null;
            }
        }

        Bson ret = null;

        if (key.startsWith(OR)) {
            if (value instanceof Collection<?>) {
                ret = Filters.or(toBsonFilters((Collection<?>) value, filterOptions));
            } else if (value instanceof Condition) {
                ret = Filters.or(toBsonFilters(List.of(value), filterOptions));
            } else if (value instanceof Map<?, ?>) {
                ret = Filters.or(toBsonFilters(List.of(value), filterOptions));
            } else {
                throw new IllegalArgumentException(String.format("%s 's filter value is not correct. expect Collection/Map/Condition:%s", OR, value));
            }
        } else if (key.startsWith(AND)) {
            if (value instanceof Collection<?>) {
                ret = Filters.and(toBsonFilters((Collection<?>) value, filterOptions));
            } else if (value instanceof Condition) {
                ret = Filters.and(toBsonFilters(List.of(value), filterOptions));
            } else if (value instanceof Map<?, ?>) {
                ret = Filters.and(toBsonFilters(List.of(value), filterOptions));
            } else {
                throw new IllegalArgumentException(String.format("%s 's filter value is not correct. expect Collection/Map/Condition:%s", AND, value));
            }

        } else if (key.startsWith(NOT)) {
            if (value instanceof Collection<?>) {
                ret = Filters.nor(toBsonFilters((Collection<?>) value, filterOptions));
            } else if (value instanceof Condition) {
                ret = Filters.nor(toBsonFilter((Condition) value, filterOptions));
            } else if (value instanceof Map<?, ?>) {
                ret = Filters.nor(toBsonFilter((Map<String, Object>) value, filterOptions));
            } else {
                throw new IllegalArgumentException(String.format("%s 's filter value is not correct. expect Collection/Map/Condition:%s", NOT, value));
            }
        } else if (key.startsWith(ELEM_MATCH)) {
            if (value instanceof Map<?, ?>) {
                var map = (Map<String, Object>) value;

                var mapsByJoinKey = extractMaps4ElemMatch(map, filterOptions.join);

                var elemConds = new ArrayList<Bson>();

                for (var entry : mapsByJoinKey.entrySet()) {
                    var elemKey = entry.getKey();
                    var elemValue = entry.getValue();

                    if (MapUtils.isEmpty(elemValue)) {
                        continue;
                    }

                    if (StringUtils.isNotEmpty(elemKey)) {
                        // elem match for a sub array

                        // Remove "children." prefix, e.g. "children.grade" -> "grade", in order to obtain a correct filter under $elemMatch
                        var subMap = elemValue.entrySet()
                                .stream()
                                .collect(Collectors.toMap(
                                        en -> StringUtils.removeStart(en.getKey(), elemKey + "."),
                                        Map.Entry::getValue
                                ));

                        elemConds.add(Filters.elemMatch(elemKey, toBsonFilter(subMap, filterOptions)));
                    } else {
                        // normal and filters
                        elemConds.add(Filters.and(toBsonFilter(elemValue, filterOptions)));
                    }
                }

                if (elemConds.isEmpty()) {
                    return trueBsonFilter();
                }
                ret = elemConds.size() > 1 ? Filters.and(elemConds) : elemConds.get(0);

            } else {
                throw new IllegalArgumentException(String.format("%s 's filter value is not correct. expect Map:%s", ELEM_MATCH, value));
            }
        } else if (key.startsWith(EXPRESSION)) {
            // support an expression as a query filter
            // e.g.: Condition.filter("$EXPRESSION exp1", "c.age / 10 < ARRAY_LENGTH(c.skills)");

            if (value instanceof String) {
                //only support expression written in string
                ret = new Document(ExpressionConvertUtil.convert((String) value));

            } else {
                throw new IllegalArgumentException(String.format("%s 's filter value is not correct. expect String:%s", EXPRESSION, value));
            }

        }
        return ret;
    }

    /**
     * extract sub maps for elem match
     *
     * <pre>
     *     input(map): {"children.grade": 1, "children.age > ": 5, "parents.firstName": "Tom", "address": "NY"}
     *     input(join): ["children", "parents"]
     *
     *     output: {
     *       "children" :  {"children.grade": 1, "children.age > ": 5},  // grouping by the same joinKey
     *       "parents": {"parents.firstName": "Tom"}
     *       "" : {"address": "NY"}
     *     }
     *
     *
     * </pre>
     *
     * @param inputMap map to be extract
     * @param join     join keys set
     * @return extracted map grouping by join keys
     */
    static Map<String, Map<String, Object>> extractMaps4ElemMatch(Map<String, Object> inputMap, Set<String> join) {

        if (MapUtils.isEmpty(inputMap)) {
            return Map.of();
        }

        if (join == null) {
            join = Set.of();
        }

        var ret = new LinkedHashMap<String, Map<String, Object>>();
        var copy = new LinkedHashMap<>(inputMap);

        for (var entry : inputMap.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();

            for (var joinKey : join) {
                ret.putIfAbsent(joinKey, new LinkedHashMap<>());
                var subMap = ret.get(joinKey);
                if (StringUtils.startsWithAny(key, joinKey + ".")) {
                    subMap.put(key, value);
                    copy.remove(key);
                }
            }
        }

        ret.put("", copy);

        return ret;
    }

    /**
     * generate expression that do {"children ARRAY_CONTAINS_ANY grade" : [5, 8]}
     *
     * @param joinKey       e.g. children
     * @param operator      e.g. ARRAY_CONTAINS_ANY
     * @param filterKey     e.g. grade
     * @param value
     * @param filterOptions
     */
    static Bson generateExpression4SubQuery(String joinKey, String operator, String filterKey, Object value, FilterOptions filterOptions) {

        if (StringUtils.isEmpty(filterKey)) {
            // if just "children ARRAY_CONTAINS_ANY"
            // this is process by other method
            return null;
        }

        Collection<?> collectionValue = null;

        if (value instanceof Collection<?>) {
            collectionValue = (Collection<?>) value;
        } else {
            collectionValue = List.of(value);
        }

        Bson ret = null;
        if (StringUtils.equals(operator, "ARRAY_CONTAINS_ANY")) {
            /* use $elemMatch
            db.Families.find({
                children: {
                    $elemMatch: {
                        grade: { $in: [5, 8] }
                    }
                }
            });
             */

            ret = Filters.elemMatch(joinKey, new Document(filterKey, new Document("$in", collectionValue)));
        } else {
            // "children ARRAY_CONTAINS_ALL grade"
            /*
            db.Families.find({
                children: {
                    $all: [
                        { $elemMatch: { grade: 5 } },
                        { $elemMatch: { grade: 8 } }
                    ]
                }
            })
             */
            ret = Filters.all(joinKey,
                    collectionValue.stream().map((v) -> new Document("$elemMatch", new Document(filterKey, v))).collect(Collectors.toList()));
        }

        return ret;

    }

    /**
     * Generate bson filter for {"fieldA OR fieldB >=" : 10} using $or
     *
     * @param key
     * @param value
     * @param filterOptions
     * @return bson filter for OrExpression
     */
    static Bson generateOrExpression(String key, Object value, FilterOptions filterOptions) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }

        var matcher = simpleExpressionPattern.matcher(key);
        var operator = "";
        var keyPart = key;
        List<Map<String, Object>> subFilters = new ArrayList<>();

        if (matcher.matches()) {
            // "fieldA OR fieldB >=" pattern
            keyPart = matcher.group(1); // "fieldA OR fieldB"
            operator = matcher.group(2); // ">="
        }

        var keys = keyPart.split(" OR ");

        // generate sub filters for $or
        for (var singleKey : keys) {
            if (StringUtils.isEmpty(singleKey)) {
                continue;
            }

            singleKey = (singleKey + " " + operator).trim();
            subFilters.add(Map.of(singleKey, value));
        }

        return toBsonFilter(Map.of("$OR", subFilters), filterOptions);

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

        return toBsonFilter(cond, filterOptions);
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

        if (Condition.isTrueCondition(cond)) {
            // SQL "(1=1)"
            return trueBsonFilter();

        } else if (Condition.isFalseCondition(cond)) {
            // SQL "(1=0)"
            return falseBsonFilter();
        }

        if (!cond.negative) {
            // a normal filter
            return toBsonFilter(cond.filter, filterOptions);
        } else {
            // process a NOR filter
            return Filters.nor(toBsonFilter(cond.filter, filterOptions));
        }
    }

    /**
     * Returns the 1=1 equivalent of bson filter. ("$expr" : {"$eq": [1,1]})
     *
     * @return true bson filter
     */
    static Document trueBsonFilter() {
        return new Document("$expr", new Document("$eq", Arrays.asList(1, 1)));
    }

    /**
     * Returns the 1=0 equivalent of bson filter. ("$expr" : {"$eq": [1,0]})
     *
     * @return false bson filter
     */
    static Document falseBsonFilter() {
        return new Document("$expr", new Document("$eq", Arrays.asList(1, 0)));
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

        var field = "";
        var order = "ASC";
        for (int i = 0; i < sort.size(); i += 2) {
            field = sort.get(i);
            order = (i + 1 < sort.size()) ? sort.get(i + 1).toUpperCase() : "ASC";

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

        if (!StringUtils.equalsIgnoreCase(field, "_ts")) {
            // when sort is not _ts, we add a second sort of _ts, in order to get a more stable sort result for mongodb
            bsonSortList.add("ASC".equals(order) ? Sorts.ascending("_ts") : Sorts.descending("_ts"));
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
            if (StringUtils.isEmpty(field)) {
                // empty field is ignored
                continue;
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

