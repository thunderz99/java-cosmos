package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.FieldKey;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.TableUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @Deprecated we use PGSimpleExpression4Join instead. because PGSimpleExpression4Join is more good at returnAllSubArray=false
 *
 * A class representing simple json path expression, which is used in Condition.join query
 * <p>
 * {@code
 *  // simple expression for json path expression
 *  // ==
 *  //(data @? '$.area.city.street.rooms[*] ? (@.no == "001")'::jsonpath)
 *  // >
 *  //(data @? '$.area.city.street.rooms[*] ? (@.no > "001")'::jsonpath)
 *  // LIKE
 *  //(data @? '$.area.city.street.rooms[*] ? (@.no like_regex "^001.*")'::jsonpath)
 * }
 */
public class PGSubQueryExpression4Join implements Expression {

    /**
     * the key for JOIN for subquery. e.g. "items" part for data->'items'
     */
    public String joinKey;

    /**
     * the key for filter for subquery. e.g. "id" part for data->'items'->'id'. Maybe empty if items is List of string / number.
     */
    public String filterKey = "";
    public Object value;

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

    public Set<String> join = new LinkedHashSet<>();
    public QueryContext queryContext;

    public PGSubQueryExpression4Join() {
    }

    public PGSubQueryExpression4Join(String joinKey, String filterKey, Object value, String operator, Set<String> join, QueryContext queryContext) {
        this.joinKey = joinKey;
        this.filterKey = filterKey;
        this.value = value;
        this.operator = operator;

        this.join = join;

        Checker.checkNotNull(queryContext, "queryContext");
        this.queryContext = queryContext;
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var baseKey = ""; // e.g. "floors"
        for(var subKey : this.join) {
            Checker.checkNotBlank(subKey, "key");
            // "floors.rooms" contains "floors"
            if(StringUtils.contains(this.joinKey, subKey)){
                baseKey = subKey;
            }
        }

        if(StringUtils.isEmpty(baseKey)){
            // baseJoinKey not match, so this is a normal PGSubQueryExpression
            var exp = new PGSubQueryExpression(this.joinKey, this.filterKey, this.value, this.operator);
            return exp.toQuerySpec(paramIndex, selectAlias);
        }


        /** pattern 1, "no" is a field directly under base joinKey "rooms"
         *  // EXISTS (
         *       SELECT 1
         *       FROM jsonb_array_elements(data->'rooms') AS j1
         *       WHERE (j1->'no' ??| @param000_no)
         *     )
         */

        /** pattern 2,  "rooms.name" is a nested field under base joinKey "floors"
         *  // EXISTS (
         *       SELECT 1
         *       FROM jsonb_array_elements(data->'floors') AS j2
         *       WHERE (j2->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__0)) OR data->'rooms' @> jsonb_build_array(jsonb_build_object('name', @param000_rooms__name__1)))
         *     )
         */

        // we will support both of the above patterns

        var remainedKey = StringUtils.removeStart(this.joinKey, baseKey + ".");

        var formattedBaseKey = PGKeyUtil.getFormattedKey4JsonWithAlias(baseKey, TableUtil.DATA);
        var ret = new CosmosSqlQuerySpec();

        {
            var existsAlias = "j" + paramIndex;
            var subExp = new PGSubQueryExpression(remainedKey, this.filterKey, this.value, this.operator);
            var subQuerySpec = subExp.toQuerySpec(paramIndex, existsAlias);

            var existsClause = """
                     EXISTS (
                       SELECT 1
                       FROM jsonb_array_elements(%s) AS %s
                       WHERE %s
                     )
                    """;

            existsClause = StringUtils.removeEnd(existsClause, "\n");
            var queryText = existsClause.formatted(formattedBaseKey, existsAlias, subQuerySpec.getQueryText().trim());

            ret.setQueryText(queryText);
            ret.setParameters(subQuerySpec.getParameters());
        }

        // save for SELECT part when returnAllSubArray=false
        // see docs/postgres-find-with-join.md for details

        {
            var alias4Select = "s" + paramIndex;
            var subExp = new PGSubQueryExpression(remainedKey, this.filterKey, this.value, this.operator);
            var subQuery = subExp.toQuerySpec(paramIndex, alias4Select);

            var clause4Select = """
                     (
                       SELECT jsonb_agg(%s)
                       FROM jsonb_array_elements(%s) AS %s
                       WHERE %s
                     )
                    """;

            clause4Select = StringUtils.removeEnd(clause4Select, "\n");
            var queryText = clause4Select.formatted(alias4Select, formattedBaseKey, alias4Select, subQuery.getQueryText().trim());

            // change the param names for select, in order to avoid param name conflict

            var params4Select = new ArrayList<CosmosSqlParameter>();
            for(var param : subQuery.getParameters()) {

                var paramName = param.getName();
                String newParamName = paramName + "__for_select";

                var newParam = new CosmosSqlParameter(newParamName, param.getValue());
                params4Select.add(newParam);
                queryText = StringUtils.replace(queryText, paramName, newParamName);

            }


            var querySpec4Select = new CosmosSqlQuerySpec();
            querySpec4Select.setQueryText(queryText);
            querySpec4Select.setParameters(params4Select);

            saveSubQuery4JsonToContext(baseKey, remainedKey, querySpec4Select);
        }

        return ret;

	}


    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }

    /**
     * save for SELECT part when returnAllSubArray=false
     *
     * @param baseKey
     * @param remainedKey
     * @param querySpec
     */
    void saveSubQuery4JsonToContext(String baseKey, String remainedKey, CosmosSqlQuerySpec querySpec) {

        var subQueryList = queryContext.subQueries4Join.get(baseKey);
        if (subQueryList == null) {
            subQueryList = new ArrayList<>();
        }
        subQueryList.add(Map.of(remainedKey, querySpec));
        queryContext.subQueries4Join.put(baseKey, subQueryList);
    }


}
