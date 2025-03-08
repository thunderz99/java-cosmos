package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.PGSelectUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * A class representing simple expression in WHERE EXIST style, which is used in Condition.join query
 * <p>
 * {@code
 *  // simple expression for join, using WHERE EXIST style
 *  // ==
 *  // EXISTS (
 *       SELECT 1
 *       FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS room
 *       WHERE room->>'no' = '001'
 *     )
 *  // >
 *  // EXISTS (
 *       SELECT 1
 *       FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS room
 *       WHERE room->>'no' > '001'
 *     )
 *  // LIKE
 *  // EXISTS (
 *       SELECT 1
 *       FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') AS room
 *       WHERE room->>'no' LIKE '00%'
 *     )
 * }
 */
public class PGSimpleExpression4Join implements Expression {

	public static final Pattern binaryOperatorPattern = Pattern.compile("^\\s*(IN|=|!=|<|<=|>|>=)\\s*$");

	public String key;
	public Object value;
	public OperatorType type = OperatorType.BINARY_OPERATOR;

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

    public PGSimpleExpression4Join() {
    }

    public PGSimpleExpression4Join(String key, Object value, Set<String> join, QueryContext queryContext) {
        this.key = key;
        this.value = value;

        // for jsonPath expression, the join must not be empty
        Checker.checkNotEmpty(join, "join");
        this.join = join;

        Checker.checkNotNull(queryContext, "queryContext");
        this.queryContext = queryContext;

    }

    public PGSimpleExpression4Join(String key, Object value, String operator, Set<String> join, QueryContext queryContext) {
        this.key = key;
        this.value = value;
        this.operator = operator;
        this.type = binaryOperatorPattern.asPredicate().test(operator) ? OperatorType.BINARY_OPERATOR
                : OperatorType.BINARY_FUNCTION;

        // for jsonPath expression, the join must not be empty
        Checker.checkNotEmpty(join, "join");
        this.join = join;

        Checker.checkNotNull(queryContext, "queryContext");
        this.queryContext = queryContext;

    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var joinKey = "";
        for(var subKey : this.join) {
            Checker.checkNotBlank(subKey, "key");
            if(StringUtils.contains(this.key, subKey)){
                joinKey = subKey;
            }
        }

        if(StringUtils.isEmpty(joinKey)){
            // joinKey not match, so this is a normal PGSimpleExpression
            var exp = new PGSimpleExpression(this.key, this.value, this.operator);
            return exp.toQuerySpec(paramIndex, selectAlias);
        }


        /** pattern 1, "no" is a field directly under base joinKey "rooms"
         *  // EXISTS (
         *       SELECT 1
         *       FROM jsonb_array_elements(data->'rooms') AS j1
         *       WHERE j1->>'no' = '001'
         *     )
         */

        /** pattern 2,  "rooms.name" is a nested field under base joinKey "floors"
         *  // EXISTS (
         *       SELECT 1
         *       FROM jsonb_array_elements(data->'floors') AS j2
         *       WHERE j2->'rooms'->>'name' = 'r1'
         *     )
         */

        // we will support both of the above patterns

        var filterKey = StringUtils.removeStart(this.key, joinKey + ".");

        var formattedJoinKey = PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias);
        var ret = new CosmosSqlQuerySpec();

        {
            var existsAlias = "j" + paramIndex;
            var subExp = new PGSimpleExpression(filterKey, this.value, this.operator);
            var subQuery = subExp.toQuerySpec(paramIndex, existsAlias);

            var existsClause = """
                     EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(%s) AS %s
                       WHERE %s
                     )
                    """;

            existsClause = StringUtils.removeEnd(existsClause, "\n");
            var queryText = existsClause.formatted(formattedJoinKey, existsAlias, subQuery.getQueryText().trim());

            ret.setQueryText(queryText);
            ret.setParameters(subQuery.getParameters());
        }

        // save for SELECT part when returnAllSubArray=false
        // see docs/postgres-find-with-join.md for details

        var baseKey = joinKey;
        var remainedJoinKey = "";
        // var filterKey = filterKey;
        // var paramIndex = paramIndex
        var subExp = new PGSimpleExpression(filterKey, this.value, this.operator);

        PGSelectUtil.saveQueryInfo4Join(queryContext, baseKey, paramIndex, subExp);

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
