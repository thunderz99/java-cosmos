package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.impl.postgres.util.PGConditionUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.PGSelectUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A class representing ElemMatch expression in WHERE EXIST style, which is used in Condition.join query
 * <p>
 * {@code
 *  // ElemMatch expression for join, using WHERE EXIST style, will AND multiple sub conditions
 *  // Condition.filter("$ELEM_MATCH", Map.of("room.no", "001", "room.name", "room-01"))
 *  // EXISTS (
 *       SELECT 1
 *       FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS room
 *       WHERE (room->>'no' = '001')
 *         AND (room->>'name' = 'room-01')
 *     )
 *  //
 *  }
 */
public class PGElemMatchExpression4Join implements Expression {

    public static final Pattern binaryOperatorPattern = Pattern.compile("^\\s*(IN|=|!=|<|<=|>|>=)\\s*$");

    public String key;
    public Map<String, Object> subFilters = new LinkedHashMap<>();
    public Condition.OperatorType type = Condition.OperatorType.BINARY_OPERATOR;

    public Set<String> join = new LinkedHashSet<>();
    public QueryContext queryContext = QueryContext.create();

    /**
     * Default is empty, which means the default operator based on filter's key and value
     *
     *     The difference between "=" and the default operator "":
     *     <div>e.g.</div>
     *     <ul>
     *     	 <li>{@code {"status": ["A", "B"]} means status is either A or B } </li>
     *       <li>{@code {"status =": ["A", "B"]} means status equals ["A", "B"] } </li>
     *     </ul>
     */
    public String operator = "";

    public PGElemMatchExpression4Join() {
    }

    public PGElemMatchExpression4Join(String key, Map<String, Object> subFilters, Set<String> join, QueryContext queryContext) {
        this.key = key;
        this.subFilters = subFilters;

        // for jsonPath expression, the join must not be empty
        Checker.checkNotEmpty(join, "join");
        this.join = join;

        Checker.checkNotNull(queryContext, "queryContext");
        this.queryContext = queryContext;

    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        // get the base joinKey
        var joinKey = "";
        for(var subKey : this.join) {
            Checker.checkNotBlank(subKey, "key");
            for(var entry : this.subFilters.entrySet()){
                if(StringUtils.contains(entry.getKey(), subKey)){
                    joinKey = subKey;
                }
            }
        }

        // when using $ELEM_MATCH, the joinKey must not be empty
        if(StringUtils.isEmpty(joinKey)){
            throw new IllegalArgumentException("joinKey cannot be empty when using $ELEM_MATCH, key: " + this.key + ", value: " + this.subFilters);
        }

        // extract subExpressions(SimpleExpression) from subFilters(the value part of $ELEM_MATCH)
        var subExpressions = new ArrayList<Expression>();
        for(var entry : this.subFilters.entrySet()) {

            var key = entry.getKey();
            var value = entry.getValue();

            var filterKey = StringUtils.removeStart(key, joinKey + ".");

            var exp = PGConditionUtil.parse(filterKey, value, Set.of(), this.queryContext);
            subExpressions.add(exp);
        }

        // let's generate the querySpec
        var ret = new CosmosSqlQuerySpec();

        var formattedJoinKey = PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias);
        var existsAlias = "j" + paramIndex;

        // the WHERE part should contain multiple sub conditions
        var existsClause = """
                     EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(%s) AS %s
                       WHERE %s
                     )
                    """;
        existsClause = StringUtils.removeEnd(existsClause, "\n");

        // multiple sub queries joined by AND
        var subQueries = subExpressions.stream().map(exp -> exp.toQuerySpec(paramIndex, existsAlias)).toList();

        // join sub query texts by AND
        var queryText = subQueries.stream().map(q -> q.getQueryText().trim())
                .collect(Collectors.joining(" AND ", "(", ")"));

        ret.setQueryText(existsClause.formatted(formattedJoinKey, existsAlias, queryText));

        // params all together from sub queries
        var params = subQueries.stream().map(q -> q.getParameters()).flatMap(List::stream).collect(Collectors.toList());
        ret.setParameters(params);


        // save for SELECT part when returnAllSubArray=false
        // see docs/postgres-find-with-join.md for details

        var baseKey = joinKey;
        var remainedJoinKey = "";
        // var filterKey = filterKey;
        // var paramIndex = paramIndex

        var subExp4Select = new ArrayList<Expression>();
        for(var entry : this.subFilters.entrySet()) {

            var key = entry.getKey();
            var value = entry.getValue();

            var filterKey = StringUtils.removeStart(key, joinKey + ".");

            var exp = PGConditionUtil.parse(filterKey, value, Set.of(), this.queryContext);
            subExp4Select.add(exp);
            PGSelectUtil.saveQueryInfo4Join(queryContext, baseKey, paramIndex, exp);

        }



        return ret;

    }

    void buildBinaryFunctionDetails(CosmosSqlQuerySpec querySpec, String jsonbPath, String jsonbKey, String paramName, Object paramValue, List<CosmosSqlParameter> params, String selectAlias) {

        var queryText = " %s @?? %s::jsonpath".formatted(selectAlias, paramName);

        switch (this.operator.toUpperCase()) {
            case "STARTSWITH"-> {
                querySpec.setQueryText(queryText);

                // in the json path expression, do not need to use ??, just use ? is ok
                // in json path expression, "STARTSWITH" is "starts with"
                // $.area.city.street.rooms[*] ? (@.no == "001")
                var valuePart = (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;

                var value = " (%s ? (%s starts with %s))".formatted(jsonbPath, jsonbKey, valuePart);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);

            }
            case "CONTAINS" -> {
                // use like_regex
                querySpec.setQueryText(queryText);

                // in json path expression, we use "like_regex" for "CONTAINS"

                var valuePart = ".*\s.*".formatted(paramValue);

                var value = " (%s ? (%s like_regex \"%s\"))".formatted(jsonbPath, jsonbKey, valuePart);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
            case "LIKE"-> {
                // use like_regex
                querySpec.setQueryText(queryText);

                // in json path expression, we use "like_regex" for "LIKE"

                var regexValue = paramValue.toString()
                        .replace("%", ".*")  // % to match any number of characters
                        .replace("_", ".");  // _ to match exactly one character

                var value = " (%s ? (%s like_regex \"%s\"))".formatted(jsonbPath, jsonbKey, regexValue);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
            case "REGEXMATCH" -> {
                // use like_regex
                querySpec.setQueryText(queryText);

                // in json path expression, we use "like_regex" for "REGEXMATCH"

                var value = " (%s ? (%s like_regex \"%s\"))".formatted(jsonbPath, jsonbKey, paramValue);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
            case "ARRAY_CONTAINS" -> {
                // use "@[*] == 2" or "@[*] == \"A\""

                querySpec.setQueryText(queryText);

                var jsonbKey4Array = jsonbKey + "[*]";

                var valuePart =  (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;

                // use ==
                var value = " (%s ? (%s == %s))".formatted(jsonbPath, jsonbKey, valuePart);

                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);

            }
            default -> {
                querySpec.setQueryText(queryText);

                var valuePart =  (paramValue instanceof String strValue) ? "\"%s\"".formatted(strValue) : paramValue;
                var value = " (%s ? (%s %s %s))".formatted(jsonbPath, jsonbKey, this.operator, valuePart);
                var param = Condition.createSqlParameter(paramName, value);
                params.add(param);
            }
        }


    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }


}
