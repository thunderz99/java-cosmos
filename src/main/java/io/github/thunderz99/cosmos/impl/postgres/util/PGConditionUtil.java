package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.condition.*;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.condition.*;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.util.AggregateUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.FieldNameUtil;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.github.thunderz99.cosmos.impl.postgres.util.PGSelectUtil.generateSelect;

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


        var initialText = String.format(" FROM %s.%s\n", schema, table);
        var initialParams = new ArrayList<CosmosSqlParameter>();
        var initialConditionIndex = new AtomicInteger(0);
        var initialParamIndex = new AtomicInteger(0);

        var queryContext = QueryContext.create();
        var filterQuery = generateFilterQuery(cond, initialText, initialParams, initialConditionIndex, initialParamIndex, queryContext);

        var queryText = filterQuery.queryText;
        var params = filterQuery.params;

        // select
        var select = generateSelect(cond, queryContext, params);
        queryText.insert(0, "SELECT %s\n".formatted(select.selectPart));

        // with clause for join and fields not empty
        if(StringUtils.isNotEmpty(select.withClause)){

            /**
             * withClause:
             *
             * WITH filtered_data AS (
             * %s
             * )
             * SELECT
             *   projectionPart
             * FROM filtered_data
             */

            var withClause = select.withClause.formatted(queryText);
            queryText = new StringBuilder(withClause);
        }

        // sort
        if (!CollectionUtils.isEmpty(cond.sort) && cond.sort.size() > 1) {
            var sorts = buildSorts(cond.sort);
            queryText.append("\n").append(sorts);
        }

        // offset and limit
        queryText.append(String.format(" OFFSET %d LIMIT %d", cond.offset, cond.limit));

        return new CosmosSqlQuerySpec(queryText.toString(), params);

    }

    /**
     * Generate an aggregate query spec for postgres from a Condition obj
     *
     * @param coll
     * @param cond
     * @param aggregate
     * @param partition
     * @return querySpec for postgres
     */
    public static CosmosSqlQuerySpec toQuerySpec4Aggregate(String coll, Condition cond, Aggregate aggregate,String partition) {

        // When rawSql is set, other filter / limit / offset / sort will be ignored.
        if (cond.rawQuerySpec != null) {
            return cond.rawQuerySpec;
        }

        // we will modify the condition, so make a copy in order to avoid side effect
        cond = cond.copy();
        cond.returnAllSubArray = true;

        var schema = TableUtil.checkAndNormalizeValidEntityName(coll);
        var table = TableUtil.checkAndNormalizeValidEntityName(partition);

        // select
        var select = generateAggregateSelect(aggregate);

        var initialText = String.format("SELECT %s\n FROM %s.%s\n", select, schema, table);
        var initialParams = new ArrayList<CosmosSqlParameter>();
        var initialConditionIndex = new AtomicInteger(0);
        var initialParamIndex = new AtomicInteger(0);

        var queryContext = QueryContext.create();
        var filterQuery = generateFilterQuery(cond, initialText, initialParams, initialConditionIndex, initialParamIndex, queryContext);

        var queryText = filterQuery.queryText;
        var params = filterQuery.params;

        // group by
        if (CollectionUtils.isNotEmpty(aggregate.groupBy)) {
            var groupBy = aggregate.groupBy.stream().map(g -> PGKeyUtil.getFormattedKey(g)).collect(Collectors.joining(", "));
            queryText.append("\n GROUP BY ").append(groupBy);
        }

        // sort (inner sort will be ignored for aggregate)
        // no need to deal with

        // offset and limit will be set and the following condition
        // 1. groupBy is enabled
        // 2. outer query is null. if outer query is enabled, setting inner offset / limit will cause sql exception in cosmosdb
        if (CollectionUtils.isNotEmpty(aggregate.groupBy) && aggregate.condAfterAggregate == null) {
            queryText.append(String.format("\n OFFSET %d LIMIT %d", cond.offset, cond.limit));
        }


        // TODO: condition after aggregation

        return new CosmosSqlQuerySpec(queryText.toString(), params);

    }

    /**
     * generate select parts for aggregate
     *
     * @param aggregate
     * @return
     */
    static String generateAggregateSelect(Aggregate aggregate) {

        Checker.checkNotNull(aggregate, "aggregate");

        var select = new ArrayList<String>();

        // $1 $2 $3... used for aggregate function or group by that without alias "e.g. COUNT(1)" without " AS facetCount"
        var columnIndex = 1;

        if (StringUtils.isNotEmpty(aggregate.function)) {

            var functionParts = aggregate.function.split(",");

            // Add accumulators for each aggregate function
            for (var functionPart : functionParts) {

                // for functionPart = "SUM(c.room.area) AS areaSum"
                // for functionPart = "COUNT(1) AS facetCount"

                // { SUM(c.room.area), areaSum }
                // { COUNT(1), facetCount }
                var functionAndAlias = AggregateUtil.extractFunctionAndAlias(functionPart);

                // SUM(c.room.area)
                // COUNT(1)
                var function = functionAndAlias.getLeft();

                // areaSum
                // facetCount
                var alias = functionAndAlias.getRight();

                // c.room.area
                // 1
                var field = AggregateUtil.extractFieldFromFunction(function);

                // room.area
                var dotFieldName = FieldNameUtil.convertToDotFieldName(field);

                if(StringUtils.isEmpty(alias)){
                    // area
                    alias = PGAggregateUtil.getSimpleName(dotFieldName);

                    if(StringUtils.equalsAny(alias, "1", "*")){
                        // $1
                        alias = "$" + columnIndex++;
                    }
                }


                var formattedKey = "";
                if(StringUtils.equals(function, field)){
                    // c['address']['state'] as result
                    // just a simple query without aggregate function
                    // data->'address'->>'state'
                    formattedKey = PGKeyUtil.getFormattedKey(dotFieldName);

                } else if(StringUtils.containsIgnoreCase(function, "array_length(")) {
                    // array_length works for a jsonb array
                    // data->'floor'->'rooms'  as a jsonb array
                    formattedKey = PGKeyUtil.getFormattedKey4JsonWithAlias(dotFieldName, TableUtil.DATA);
                } else {
                    // (data->'room'->>'area')::numeric
                    // TODO deal with other type(e.g string)
                    formattedKey = "(%s)::numeric".formatted(PGKeyUtil.getFormattedKey(dotFieldName));
                }

                if(StringUtils.equalsAny(field, "1", "*")){
                    // do nothing, the COUNT(1) should not be changed
                } else {
                    // SUM(rooms.area) -> SUM(data->'room'->>'area')
                    function = StringUtils.replace(function, field, formattedKey);
                }

                // for array_length
                // array_length not work in postgres JSONB column, use jsonb_array_length instead
                if(StringUtils.containsIgnoreCase(function, "array_length(")){
                    function = StringUtils.replaceIgnoreCase(function, "array_length(", "jsonb_array_length(");
                }

                select.add("%s AS \"%s\"".formatted(function, alias));
            }

        }

        if (CollectionUtils.isNotEmpty(aggregate.groupBy)) {

            for(var groupBy : aggregate.groupBy){
                if(StringUtils.isBlank(groupBy)){
                    continue;
                }
                var dotFieldName = FieldNameUtil.convertToDotFieldName(groupBy);

                var simpleName = PGAggregateUtil.getSimpleName(dotFieldName);

                var formattedKey = PGKeyUtil.getFormattedKey(dotFieldName);
                select.add("%s AS \"%s\"".formatted(formattedKey, simpleName));
            }

        }

        if (select.isEmpty()) {
            throw new IllegalArgumentException("aggregate and function cannot both be empty");
        }

        return select.stream().collect(Collectors.joining(", "));
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

        // TODO: sort by integer
        var ret = sortMap.entrySet().stream()
                .map(entry -> String.format(" %s %s", PGKeyUtil.getFormattedKeyWithAlias(entry.getKey(), TableUtil.DATA, ""), entry.getValue().toUpperCase()))
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

        var queryContext = QueryContext.create();
        var filterQuery = generateFilterQuery(cond, initialText, initialParams, initialConditionIndex, initialParamIndex, queryContext);

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
     * @param queryContext context info for query, especially using join
     */
    static FilterQuery generateFilterQuery(Condition cond, String selectPart, List<CosmosSqlParameter> params,
                                    AtomicInteger conditionIndex, AtomicInteger paramIndex, QueryContext queryContext) {

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

        var join = new LinkedHashSet(cond.join);

        var selectAlias = TableUtil.DATA;

        for (var entry : cond.filter.entrySet()) {

            if (StringUtils.isEmpty(entry.getKey())) {
                // ignore when key is empty
                continue;
            }

            var subFilterQueryToAdd = "";

            if (entry.getKey().startsWith(SubConditionType.AND)) {
                // sub query AND
                var subQueries = extractSubQueries(cond, entry.getValue());
                subFilterQueryToAdd = generateFilterQuery4List(subQueries, "AND", params, conditionIndex, paramIndex, queryContext);
            } else if (entry.getKey().startsWith(SubConditionType.OR)) {
                // sub query OR
                var subQueries = extractSubQueries(cond, entry.getValue());
                subFilterQueryToAdd = generateFilterQuery4List(subQueries, "OR", params, conditionIndex, paramIndex, queryContext);

            } else if (entry.getKey().startsWith(SubConditionType.NOT)) {
                // sub query NOT
                var subQueries = extractSubQueries(cond, entry.getValue());
                if (CollectionUtils.isNotEmpty(subQueries)) {
                    var subQueryWithNot = Condition.filter(SubConditionType.AND, subQueries).join(cond.join).not();
                    // recursively generate the filterQuery with negative flag true
                    var filterQueryWithNot = generateFilterQuery(subQueryWithNot, "", params, conditionIndex, paramIndex, queryContext);
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
                var exp = parse(entry.getKey(), entry.getValue(), join, queryContext);
                var expQuerySpec = exp.toQuerySpec(paramIndex, selectAlias);
                subFilterQueryToAdd = expQuerySpec.getQueryText();
                params.addAll(expQuerySpec.getParameters());

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
     * @param parentCond
     * @param value
     */
    static List<Condition> extractSubQueries(Condition parentCond, Object value) {
        if (value == null) {
            return List.of();
        }

        if (value instanceof Condition || value instanceof Map<?, ?>) {
            // single condition
            return List.of(extractSubQuery(parentCond, value));
        } else if (value instanceof List<?>) {
            // multi condition
            var listValue = (List<Object>) value;
            return listValue.stream().map(v -> extractSubQuery(parentCond, v)).filter(Objects::nonNull).collect(Collectors.toList());
        }

        return List.of();
    }

    /**
     * extract subQuery for SUB_COND_AND / SUB_COND_OR 's filter value, single condition only.
     *
     * @param parentCond
     * @param value
     */
    static Condition extractSubQuery(Condition parentCond, Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Condition) {
            // single condition
            return ((Condition) value).join(parentCond.join);
        } else if (value instanceof Map<?, ?>) {
            // single condition in the form of map
            return new Condition(JsonUtil.toMap(value)).join(parentCond.join);
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
     * @param queryContext   context info for query. especially using join
     * @return query text
     */
    static String generateFilterQuery4List(List<Condition> conds, String operator4SubConds, List<CosmosSqlParameter> params, AtomicInteger conditionIndex, AtomicInteger paramIndex, QueryContext queryContext) {
        List<String> subTexts = new ArrayList<>();
        //List<String> originSubTexts = new ArrayList<>();

        for (var subCond : conds) {

            var subFilterQuery = generateFilterQuery(subCond,"", params, conditionIndex,
                    paramIndex, queryContext);

            var subText = removeConnectPart(subFilterQuery.queryText.toString());
            subTexts.add(subText);

            params = subFilterQuery.params;
            conditionIndex = subFilterQuery.conditionIndex;
            paramIndex = subFilterQuery.paramIndex;

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
     * @param queryContext context of the Expression(e.g. under join which uses a json path expression)
     * @return expression for WHERE clause
     */
    public static Expression parse(String key, Object value, Set<String> join, QueryContext queryContext) {

        //simple expression
        var simpleMatcher = Condition.simpleExpressionPattern.matcher(key);
        if (simpleMatcher.find()) {
            var parsedKey = simpleMatcher.group(1);
            var operator = simpleMatcher.group(2);
            return parseExpression4SimpleMatcher(key, parsedKey, value, operator, join, queryContext);
        }

        //subquery expression
        var subqueryMatcher = Condition.subQueryExpressionPattern.matcher(key);

        if (subqueryMatcher.find()) {
            var joinKey = subqueryMatcher.group(1);
            var filterKey = subqueryMatcher.group(3);
            var operator = subqueryMatcher.group(2);

            if (CollectionUtils.isEmpty(join)) {
                return new PGSubQueryExpression(joinKey, filterKey, value, operator);
            } else {
                return new PGSubQueryExpression4Join(joinKey, filterKey, value, operator, join, queryContext);
            }
        }

        //default key / value expression
        // TODO PGOrExpressions for join

        return parseExpression4SimpleMatcher(key, key, value, "", join, queryContext);
    }

    /**
     * parse a key / value / operator to a valid simple expression
     *
     * @param key original key in the filter
     * @param parsedKey parsed key in the filter
     * @param value value in the filter
     * @param operator operator for the expression
     * @param queryContext context of the Expression(e.g. under join which uses a json path expression)
     * @return
     */
    static Expression parseExpression4SimpleMatcher(String key, String parsedKey, Object value, String operator, Set<String> join, QueryContext queryContext) {
        if (key.contains(" OR ")) {
            return new PGOrExpressions(parsedKey, value);
        } else {
            if (CollectionUtils.isEmpty(join)) {
                return new PGSimpleExpression(parsedKey, value, operator);
            } else {
                return new PGSimpleExpression4Join(parsedKey, value, operator, join, queryContext);
            }
        }
    }

}

