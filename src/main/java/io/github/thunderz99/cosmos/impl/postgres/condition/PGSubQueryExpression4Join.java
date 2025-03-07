package io.github.thunderz99.cosmos.impl.postgres.condition;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Condition.OperatorType;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.FieldKey;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.impl.postgres.util.PGSelectUtil;
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
 * A class representing SubQuery(ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL) expression for join, which is used in "Condition.join" query
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

        // e.g. "rooms"
        var remainedJoinKey = StringUtils.removeStart(this.joinKey, baseKey + ".");

        // e.g. data->"floors"
        var formattedBaseKey = PGKeyUtil.getFormattedKey4JsonWithAlias(baseKey, TableUtil.DATA);
        var ret = new CosmosSqlQuerySpec();

        {
            var existsAlias = "j" + paramIndex;

            // remainedJoinKey = "rooms"
            // filterKey = "name"
            // value = ["r1", "r2"]
            // operator = "ARRAY_CONTAINS_ANY"
            var subExp = new PGSubQueryExpression(remainedJoinKey, this.filterKey, this.value, this.operator);
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

        // var baseKey = baseKey;
        // var remainedJoinKey = remainedJoinKey;
        // var filterKey = filterKey;
        // var paramIndex = paramIndex
        var subExp4Select = new PGSubQueryExpression(remainedJoinKey, this.filterKey, this.value, this.operator);
        PGSelectUtil.saveQueryInfo4Join(queryContext, baseKey, paramIndex, subExp4Select);

        return ret;

	}


    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }


}
