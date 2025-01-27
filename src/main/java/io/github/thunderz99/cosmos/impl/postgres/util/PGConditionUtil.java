package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import io.github.thunderz99.cosmos.condition.*;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.condition.PGOrExpressions;
import io.github.thunderz99.cosmos.impl.postgres.condition.PGSimpleExpression;
import io.github.thunderz99.cosmos.impl.postgres.condition.PGSimpleExpression4JsonPath;
import io.github.thunderz99.cosmos.impl.postgres.condition.PGSubQueryExpression;
import io.github.thunderz99.cosmos.impl.postgres.dto.PGFilterOptions;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A util class to convert condition's filter/sort/limit/offset to bson filter/sort/limit/offset for mongo
 */
public class PGConditionUtil {

    private static final Logger log = LoggerFactory.getLogger(PGConditionUtil.class);

    /**
     * Generate a query spec for postgres from a Condition obj
     * @param coll
     * @param cond
     * @param partition
     * @return querySpec for postgres
     */
    public static CosmosSqlQuerySpec toQuerySpec(String coll, Condition cond, String partition) {

        // When rawSql is set, other filter / limit / offset / sort will be ignored.
        if (cond.rawQuerySpec != null) {
            return cond.rawQuerySpec;
        }

        var schema = TableUtil.checkAndNormalizeValidEntityName(coll);
        var table = TableUtil.checkAndNormalizeValidEntityName(partition);


        var initialText = String.format("FROM %s.%s", schema, table);
        var initialParams = new ArrayList<CosmosSqlParameter>();
        var initialConditionIndex = new AtomicInteger(0);
        var initialParamIndex = new AtomicInteger(0);

        var filterQuery = generateFilterQuery(cond, initialText, initialParams, initialConditionIndex, initialParamIndex, TableUtil.DATA);

        var queryText = filterQuery.queryText;
        var params = filterQuery.params;

        // select
        var select = generateSelect(cond);
        queryText.insert(0, "SELECT %s ".formatted(select));

        // sort
        if (!CollectionUtils.isEmpty(cond.sort) && cond.sort.size() > 1) {
            var sorts = buildSorts(cond.sort);
            queryText.append(sorts);
        }

        // offset and limit
        queryText.append(String.format(" OFFSET %d LIMIT %d", cond.offset, cond.limit));

        return new CosmosSqlQuerySpec(queryText.toString(), params);

    }


    /**
     * Generate the sort queryText
     *
     * @param sort list of sort key and orders
     * @return sort queryText
     */
    static String buildSorts(List<String> sort) {

        if(CollectionUtils.isEmpty(sort)){
            return "";
        }

        var sortMap = new LinkedHashMap<String, String>();

        // record the first order "ASC" or "DESC"
        var firstOrder = "";

        for (int i = 0; i < sort.size(); i++) {
            if (i % 2 == 0) {
                sortMap.put(sort.get(i), sort.get(i + 1));
            }
            if(i == 1){
                firstOrder = sort.get(i);
            }
        }


        if(!sortMap.keySet().contains("_ts")){
            // when sort does not include "_ts", we add a second sort of _ts, in order to get a more stable sort result for postgres
            sortMap.put("_ts", firstOrder);
        }

        var ret = sortMap.entrySet().stream()
                .map(entry -> String.format(" %s %s", PGKeyUtil.getFormattedKey(entry.getKey()), entry.getValue().toUpperCase()))
                .collect(Collectors.joining(",", " ORDER BY", ""));
        return ret;
    }

    /**
     * Generate a count query spec for postgres from a Condition obj
     * @param coll
     * @param cond
     * @param partition
     * @return querySpec for postgres
     */
    public static CosmosSqlQuerySpec toQuerySpecForCount(String coll, Condition cond, String partition) {

        // When rawSql is set, other filter / limit / offset / sort will be ignored.
        if (cond.rawQuerySpec != null) {
            return cond.rawQuerySpec;
        }

        var schema = TableUtil.checkAndNormalizeValidEntityName(coll);
        var table = TableUtil.checkAndNormalizeValidEntityName(partition);

        var select = "COUNT(*)";

        var initialText = String.format("SELECT %s FROM %s.%s", select, schema, table);
        var initialParams = new ArrayList<CosmosSqlParameter>();
        var initialConditionIndex = new AtomicInteger(0);
        var initialParamIndex = new AtomicInteger(0);

        var filterQuery = generateFilterQuery(cond, initialText, initialParams, initialConditionIndex, initialParamIndex, TableUtil.DATA);

        var queryText = filterQuery.queryText;
        var params = filterQuery.params;

        // sort
        // not needed for count

        // offset and limit
        // not needed for count

        return new CosmosSqlQuerySpec(queryText.toString(), params);

    }


    /**
     * filter parts
     *
     * @param cond        condition
     * @param selectPart  queryText
     * @param params      params
     * @param selectAlias field name for "data->>'name'"
     */
    static FilterQuery generateFilterQuery(Condition cond, String selectPart, List<CosmosSqlParameter> params,
                                    AtomicInteger conditionIndex, AtomicInteger paramIndex, String selectAlias) {

        // process raw sql
        if (cond.rawQuerySpec != null) {
            conditionIndex.getAndIncrement();
            params.addAll(cond.rawQuerySpec.getParameters());
            String rawQueryText = processNegativeQuery(cond.rawQuerySpec.getQueryText(), cond.negative);
            return new FilterQuery(rawQueryText,
                    params, conditionIndex, paramIndex);
        }

        // process filters

        var queryTexts = new ArrayList<String>();

        // filter parts
        var connectPart = getConnectPart(conditionIndex);

        var filterOption = PGFilterOptions.create().join(Sets.newHashSet(cond.join));

        for (var entry : cond.filter.entrySet()) {

            if (StringUtils.isEmpty(entry.getKey())) {
                // ignore when key is empty
                continue;
            }

            var subFilterQueryToAdd = "";

            if (entry.getKey().startsWith(SubConditionType.AND)) {
                // sub query AND
                var subQueries = extractSubQueries(entry.getValue());
                subFilterQueryToAdd = generateFilterQuery4List(subQueries, "AND", params, conditionIndex, paramIndex, cond.join);

            } else if (entry.getKey().startsWith(SubConditionType.OR)) {
                // sub query OR
                var subQueries = extractSubQueries(entry.getValue());
                subFilterQueryToAdd = generateFilterQuery4List(subQueries, "OR", params, conditionIndex, paramIndex, cond.join);

            } else if (entry.getKey().startsWith(SubConditionType.NOT)) {
                // sub query NOT
                var subQueries = extractSubQueries(entry.getValue());
                if (CollectionUtils.isNotEmpty(subQueries)) {
                    var subQueryWithNot = Condition.filter(SubConditionType.AND, subQueries).join(cond.join).not();
                    // recursively generate the filterQuery with negative flag true
                    var filterQueryWithNot = generateFilterQuery(subQueryWithNot, "", params, conditionIndex, paramIndex, selectAlias);
                    subFilterQueryToAdd = " " + removeConnectPart(filterQueryWithNot.queryText.toString());
                }
            } else if (entry.getKey().startsWith(SubConditionType.EXPRESSION)) {
                // support expression using combination of function and the basic operators
                // e.g.: Condition.filter("$EXPRESSION exp1", "c.age / 10 < ARRAY_LENGTH(c.skills)");

                if ((entry.getValue() instanceof String)) {
                    //only support expression write in string
                    var expressionStr = (String) entry.getValue();
                    if (StringUtils.isNotEmpty(expressionStr)) {
                        subFilterQueryToAdd = String.format(" (%s)", expressionStr);
                    }
                }

            } else {
                // normal "key = value" expression
                var exp = parse(entry.getKey(), entry.getValue(), filterOption);
                var expQuerySpec = exp.toQuerySpec(paramIndex, selectAlias);
                subFilterQueryToAdd = expQuerySpec.getQueryText();
                params.addAll(expQuerySpec.getParameters());

                // TODO join(not needed any more. to be deleted)
//                saveOriginJoinCondition(cond, subFilterQueryToAdd);
//                subFilterQueryToAdd = buildArrayJoinQueryText(cond, entry.getKey(), subFilterQueryToAdd, expQuerySpec.params);
//                params.addAll(expQuerySpec.getParameters());
            }

            if (StringUtils.isNotEmpty(subFilterQueryToAdd)) {
                queryTexts.add(subFilterQueryToAdd);
                conditionIndex.getAndIncrement();
            }
        }
        var queryText = String.join(" AND", queryTexts);

        //negative
        queryText = processNegativeQuery(queryText, cond.negative);

        //add WHERE part
        if (StringUtils.isNotBlank(queryText)) {
            queryText = connectPart + queryText;
        }

        //add SELECT part
        queryText = selectPart + queryText;

        return new FilterQuery(queryText, params, conditionIndex, paramIndex);
    }


    /**
     * If the expression contains join part, translate it to join query
     * e.g. "(data->'items'->>'name' == @param000_items) -> "(data @?? '$.items[*] ? (@.name == \"%s\")"')
     * @param cond the filter condition
     * @param keyWithOps the field name
     * @param subFilterQueryToAdd the current query text
     * @param params the sql params
     * @return the translated query text
     */
    static String buildArrayJoinQueryText(Condition cond, String keyWithOps, String subFilterQueryToAdd, List<CosmosSqlParameter> params) {

        Checker.checkNotNull(cond, "cond");
        Checker.checkNotBlank(keyWithOps, "keyWithOps");
        Checker.checkNotBlank(subFilterQueryToAdd, "subFilterQueryToAdd");
        Checker.checkNotNull(params, "params");

        // because the keyWithOps contains "> >= < <=", we have to remove the ops part
        // e.g. "children.grade >", we should remove the " >" part
        var keyParts = keyWithOps.split("\\s+");
        var key = keyParts[0];

        for (String joinPart : cond.join) {
            if(key.contains(joinPart) || subFilterQueryToAdd.contains(PGKeyUtil.getFormattedKey4Json(joinPart))){

                // key format used in jsonb_path. e.g. "$.area.city.street.rooms[*]" or "$.\"room*no-01\"[*]"
                var keyPair = PGKeyUtil.getJsonbPathKey(joinPart, key);
                var jsonbPath = keyPair.getLeft();
                var jsonbKey = keyPair.getRight();

                for(var i=0; i< params.size(); i++){
                    var paramName = params.get(i).getName();
                    var value = params.get(i).getValue();
                    var valuePart = "%s";
                    if(value instanceof String strValue){
                        // escape the string. e.g.  "(@.no == \"%s\")";
                        valuePart = "\"%s\"";
                    }

                    // replace the key. from "data->'area'->'city'->'street'->'rooms'->'no'" to "$.area.city.street.rooms[*] ? (@.no"

                    var simpleKey = PGKeyUtil.getFormattedKey(key, value);
                    if(subFilterQueryToAdd.contains(simpleKey)) {
                        // simpleKey: (data->'area'->'city'->'street'->'rooms'->>'no')::int
                        subFilterQueryToAdd = subFilterQueryToAdd.replace(simpleKey, "%s ? (%s".formatted(jsonbPath, jsonbKey));
                    } else {
                        // key4JsonOps: data->'area'->'city'->'street'->'rooms'->'no'
                        subFilterQueryToAdd = subFilterQueryToAdd.replace(PGKeyUtil.getFormattedKey4Json(key), "%s ? (%s".formatted(jsonbPath, jsonbKey));
                    }

                    // replace the paramName. from "@param001_area__city__street__rooms__no" to "\"%s\""
                    subFilterQueryToAdd = subFilterQueryToAdd.replace(paramName,valuePart+")");

                    // replace = to == for jsonb path expression
                    subFilterQueryToAdd = subFilterQueryToAdd.replace(" = "," == ");

                    // replace ?? to == for jsonb path expression
                    subFilterQueryToAdd = subFilterQueryToAdd.replace(" ?? "," == ");


                    // replace %s to the real value
                    subFilterQueryToAdd = subFilterQueryToAdd.formatted(value);

                    // the new value is a string for jsonb path
                    params.get(i).value =  new String(subFilterQueryToAdd);

                    // the new filterQuery is a query using "data @??"
                    subFilterQueryToAdd = " (data @?? %s::jsonpath)".formatted(paramName);

                }

                break;
            }
        }

        return subFilterQueryToAdd;
    }

    /**
     * Save the conditions of the join part to map.
     * @param cond the filter condition
     * @param originJoinConditionText condition text
     */
    static void saveOriginJoinCondition(Condition cond, String originJoinConditionText){
        for (String joinPart : cond.join) {
            if(originJoinConditionText.contains(PGKeyUtil.getFormattedKey(joinPart))){
                var joinCondTextList= cond.joinCondText.getOrDefault(joinPart,new ArrayList<>());
                joinCondTextList.add(originJoinConditionText);
                cond.joinCondText.put(joinPart,joinCondTextList);
                break;
            }
        }
    }

    /**
     * add negative NOT operator for queryText, if not empty
     *
     * @param queryText
     * @param negative
     * @return
     */
    static String processNegativeQuery(String queryText, boolean negative) {
        return negative && StringUtils.isNotEmpty(queryText) ?
                " NOT(" + queryText + ")" : queryText;
    }

    /**
     * extract subQueries for SUB_COND_AND / SUB_COND_OR 's filter value
     *
     * @param value
     */
    static List<Condition> extractSubQueries(Object value) {
        if (value == null) {
            return List.of();
        }

        if (value instanceof Condition || value instanceof Map<?, ?>) {
            // single condition
            return List.of(extractSubQuery(value));
        } else if (value instanceof List<?>) {
            // multi condition
            var listValue = (List<Object>) value;
            return listValue.stream().map(v -> extractSubQuery(v)).filter(Objects::nonNull).collect(Collectors.toList());
        }

        return List.of();
    }

    /**
     * extract subQuery for SUB_COND_AND / SUB_COND_OR 's filter value, single condition only.
     *
     * @param value
     */
    static Condition extractSubQuery(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Condition) {
            // single condition
            return (Condition) value;
        } else if (value instanceof Map<?, ?>) {
            // single condition in the form of map
            return new Condition(JsonUtil.toMap(value));
        } else if (value instanceof Collection<?>) {
            throw new IllegalArgumentException("Cannot convert input to a single condition. Ensure the input is a single value(not a collection)." + value);
        }
        throw new IllegalArgumentException("Invalid input. expect a condition or a map. " + value);
    }

    /**
     * @param conds          conditions
     * @param operator4SubConds         "AND", "OR"
     * @param params         sql params
     * @param conditionIndex increment index for conditions (for uniqueness of param names)
     * @param paramIndex     increment index for params (for uniqueness of param names)
     * @param join           join part of parent condition.
     * @return query text
     */
    static String generateFilterQuery4List(List<Condition> conds, String operator4SubConds, List<CosmosSqlParameter> params, AtomicInteger conditionIndex, AtomicInteger paramIndex, Set<String> join) {
        List<String> subTexts = new ArrayList<>();
        //List<String> originSubTexts = new ArrayList<>();

        for (var subCond : conds) {

            var originSubJoin = subCond.join;
            subCond.join = join;

            var subFilterQuery = generateFilterQuery(subCond,"", params, conditionIndex,
                    paramIndex, TableUtil.DATA);

            var subText = removeConnectPart(subFilterQuery.queryText.toString());
            subTexts.add(subText);

            params = subFilterQuery.params;
            conditionIndex = subFilterQuery.conditionIndex;
            paramIndex = subFilterQuery.paramIndex;

            // restore the join part
            subCond.join = originSubJoin;

        }

        var subFilterQuery = subTexts.stream().filter(t -> StringUtils.isNotBlank(t))
                .collect(Collectors.joining(" " + operator4SubConds + " ", " (", ")"));

        // remove empty sub queries
        return StringUtils.removeStart(subFilterQuery, " ()");
    }

    static String removeConnectPart(String subQueryText) {
        return StringUtils.removeStart(StringUtils.removeStart(subQueryText, " WHERE"), " AND").trim();
    }

    static String getConnectPart(AtomicInteger conditionIndex) {
        return conditionIndex.get() == 0 ? " WHERE" : " AND";
    }

    /**
     * parse key and value to generate a valid expression
     * @param key filter's key
     * @param value filter's value
     * @param filterOptions context of the Expression(e.g. under join which uses a json path expression)
     * @return expression for WHERE clause
     */
    public static Expression parse(String key, Object value, PGFilterOptions filterOptions) {

        //simple expression
        var simpleMatcher = Condition.simpleExpressionPattern.matcher(key);
        if (simpleMatcher.find()) {
            if (key.contains(" OR ")) {
                return new PGOrExpressions(simpleMatcher.group(1), value, simpleMatcher.group(2));
            } else {
                if (CollectionUtils.isEmpty(filterOptions.join)) {
                    return new PGSimpleExpression(simpleMatcher.group(1), value, simpleMatcher.group(2));
                } else {
                    return new PGSimpleExpression4JsonPath(simpleMatcher.group(1), value, simpleMatcher.group(2), filterOptions);
                }
            }
        }

        //subquery expression
        var subqueryMatcher = Condition.subQueryExpressionPattern.matcher(key);

        if (subqueryMatcher.find()) {
            return new PGSubQueryExpression(subqueryMatcher.group(1), subqueryMatcher.group(3), value, subqueryMatcher.group(2));
        }

        //default key / value expression
        if (key.contains(" OR ")) {
            return new PGOrExpressions(key, value);
        } else {
            if (CollectionUtils.isEmpty(filterOptions.join)) {
                return new PGSimpleExpression(key, value);
            } else {
                return new PGSimpleExpression4JsonPath(key, value, filterOptions);
            }
        }
    }

    /**
     * select parts generate.
     *
     * {@code
     * e.g.
     * "id", "age", "fullName.first" -> VALUE {"id":c.id, "age":c.age, "fullName": {"first": c.fullName.first}}
     * }
     *
     * @return select sql
     */
    static String generateSelect(Condition cond) {
        if (CollectionUtils.isEmpty(cond.fields)) {
            return "*";
        }
        return generateSelectByFields(cond.fields);
    }

    /**
     * Generate a select sql for input fields. Supports nested fields
     *
     * <p>
     * {@code
     * //e.g.
     * //input: ["id", "address.city"]
     * //output: id,
     *      jsonb_build_object(
     *       'address',
     *       jsonb_build_object(
     *         'city', data->'address'->'city'
     *       )
     *     ) AS "Data"
     * }
     *
     * </p>
     *
     * @param fieldsSet
     * @return
     */
    static String generateSelectByFields(Set<String> fieldsSet) {

        // converts field to data.field, excludes empty fields
        // ["id", "name", ""] -> ["data.id", "data.name"]

        var fields = fieldsSet.stream().filter(f -> StringUtils.isNotBlank(f)).map(f -> {
            if (StringUtils.containsAny(f, "{", "}", ",", "\"", "'")) {
                throw new IllegalArgumentException("field cannot contain '{', '}', ',', '\"', \"'\", field: " + f);
            }
            return TableUtil.DATA + "." + f;
        }).collect(Collectors.toCollection(LinkedHashSet::new));

        // add "id" and "data.id" fields as a must
        fields.add(TableUtil.ID);
        fields.add(TableUtil.DATA + "." + TableUtil.ID);

        // Separate top-level fields (no dot) from nested fields
        var topLevel = new LinkedHashSet<String>(); // should contain "id" only

        // should be like ["id", "name", "address.city"]
        // note that the "data." part is removed for the simplicity of the following process
        // "id" is always added to the nested fields (because we have a redundant "data.id" field, whose value is the same as "id")
        var nested  = new ArrayList<String>();
        for (var f : fields) {
            if (f.contains(".")) {
                nested.add(StringUtils.removeStart(f, TableUtil.DATA + "."));
            } else {
                topLevel.add(f);
            }
        }

        // Build a tree structure for nested fields
        var root = new Node(); // this node represents the "data" root
        for (var f : nested) {
            var parts = Arrays.asList(f.split("\\."));
            insertPath(root, parts);
        }

        // Build the list of select columns
       var selectColumns = new ArrayList<String>();

        // 1) Add the top-level fields as normal columns
        selectColumns.addAll(topLevel);

        // 2) If there are any nested fields, build the JSON expression
        if (!nested.isEmpty()) {
            var jsonExpression = buildJsonBuildObject(TableUtil.DATA, root);
            // Alias it as "Data"
            jsonExpression += " AS \"" + TableUtil.DATA + "\"";
            selectColumns.add(jsonExpression);
        }

        // Join them with commas, and add a newline for the end
        return String.join(",\n", selectColumns) + "\n";
    }

    /**
     * Recursively build a jsonb_build_object(...) expression from the Node tree.
     * The 'pathSoFar' indicates how we refer to this level in 'data', e.g. "data->'address'"
     */
    static String buildJsonBuildObject(String pathSoFar, Node node) {
        // If node has no children, it means this path is a leaf
        // so just return the pathSoFar (e.g. "data->'address'->'city'")
        if (node.children.isEmpty()) {
            return pathSoFar;
        }

        // Otherwise, build jsonb_build_object with each child
        // e.g. jsonb_build_object(
        //         'address', jsonb_build_object(
        //             'city', data->'address'->'city'
        //         )
        //      )
        List<String> pairs = new ArrayList<>();
        for (Map.Entry<String, Node> entry : node.children.entrySet()) {
            var key   = entry.getKey();
            var child   = entry.getValue();
            var childPath = pathSoFar + "->'" + key + "'";
            // Recursively build the expression for the child's subtree
            var childExpr = buildJsonBuildObject(childPath, child);
            // `'key', childExpr`
            var pair = "'" + key + "', " + childExpr;
            pairs.add(pair);
        }

        // join pairs into "jsonb_build_object('key1', expr1, 'key2', expr2, ...)"
        var joined = String.join(", ", pairs);
        return "jsonb_build_object(" + joined + ")";
    }

    /**
     * Insert a path like ["address","city"] into our tree structure.
     * 'root' node is effectively for "data".
     */
    static void insertPath(Node root, List<String> parts) {
        var current = root;
        for (String p : parts) {
            current.children.putIfAbsent(p, new Node());
            current = current.children.get(p);
        }
    }

    /**
     * Simple tree node that maps child name -> subtree
     */
    static class Node {
        Map<String, Node> children = new LinkedHashMap<>();
    }


}

