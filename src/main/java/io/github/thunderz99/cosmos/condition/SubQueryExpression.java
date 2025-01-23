package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * a class representing expression using subqueries and array operations
 *
 * {@code
 * ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL, and others which using EXISTS(SELECT VALUE x FROM x IN c.items)
 * }
 *
 * <p>
 *     see cosmosdb subquery for details
 * </p>
 */
public class SubQueryExpression implements Expression {

	public static final String ARRAY_CONTAINS_ANY = "ARRAY_CONTAINS_ANY";
	public static final String ARRAY_CONTAINS_ALL = "ARRAY_CONTAINS_ALL";

	/**
	 * the key for JOIN for subquery. e.g. "items" part for c.items
	 */
	public String joinKey;

	/**
	 * the key for filter for subquery. e.g. "id" part for c.items.id. Maybe empty if c.items is List of string / number.
	 */
	public String filterKey = "";
	public Object value;
	public String operator = "=";

    public SubQueryExpression() {
    }

    public SubQueryExpression(String joinKey, String filterKey, Object value, String operator) {
        this.joinKey = joinKey;
        this.filterKey = filterKey;
        this.value = value;
        this.operator = operator;
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var ret = new CosmosSqlQuerySpec();
        var params = new ArrayList<CosmosSqlParameter>();

        // joinKey.filterKey -> fullName.last -> @param001_fullName__last
        var key = List.of(this.joinKey, this.filterKey).stream().filter(StringUtils::isNotEmpty).collect(Collectors.joining("."));
        var paramName = ParamUtil.getParamNameFromKey(key, paramIndex.get());
        var paramValue = this.value;

        paramIndex.getAndIncrement();

        var queryText = "";
        if (ARRAY_CONTAINS_ALL.equals(this.operator)) {
            queryText = buildArrayContainsAll(this.joinKey, this.filterKey, paramName, paramValue, params);
        } else {
            queryText = buildArrayContainsAny(this.joinKey, this.filterKey, paramName, paramValue, params);
        }

        ret.setQueryText(queryText);
        ret.setParameters(params);

        return ret;
    }

    @Override
    public String toString() {
        return JsonUtil.toJson(this);
    }


    /**
     * A helper function to generate c.items ARRAY_CONTAINS_ANY List.of(item1, item2) queryText
     *
     * <pre>
     * INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
     * OUTPUT:
     * " (EXISTS(SELECT VALUE x FROM x IN c["items"] WHERE ARRAY_CONTAINS(@items_009, x)))"
     *
     *
     * INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
     * OUTPUT:
     * " (EXISTS(SELECT VALUE x FROM x IN c["items"] WHERE ARRAY_CONTAINS(@id_010, x["id"])))"
     *
     *  and add paramsValue into params
     * </pre>
     */
    static String buildArrayContainsAny(String joinKey, String filterKey, String paramName, Object paramValue, List<CosmosSqlParameter> params) {

        Checker.checkNotBlank(joinKey, "joinKey");
        Checker.checkNotNull(filterKey, "filterKey");
        Checker.checkNotBlank(paramName, "paramName");

        params.add(Condition.createSqlParameter(paramName, paramValue));

        if (paramValue instanceof Collection<?>) {
            //collection
            return String.format(" (EXISTS(SELECT VALUE x FROM x IN %s WHERE ARRAY_CONTAINS(%s, %s)))",
                    Condition.getFormattedKey(joinKey), paramName, Condition.getFormattedKey(filterKey, "x"));

        } else {
            //scalar
            return String.format(" (%s)", buildSimpleSubQuery(joinKey, filterKey, paramName));
        }
    }

    /**
     * A helper function to generate c.items ARRAY_CONTAINS_All List.of(item1, item2) queryText
     *
     * <pre>
     * INPUT: "items", "", "@items_009", ["id001", "id002"], params
     * OUTPUT:
     * " (EXISTS(SELECT VALUE x FROM x IN c["items"] WHERE x = @items_009__000)) AND EXISTS(SELECT VALUE x FROM x IN c["items"] WHERE x = @items_009_001)))"
     *
     *
     * INPUT: "tags", "name", "@tags_name_010", ["react", "java"], params
     * OUTPUT:
     * " (EXISTS(SELECT VALUE x FROM x IN c["tags"] WHERE ARRAY_CONTAINS(@tags_name_010__000, x["name"])) AND EXISTS(SELECT VALUE x FROM x IN c["tags"] WHERE ARRAY_CONTAINS(@tags_name_010__000, x["name"])))"
     *
     *  and add paramsValue into params
     * </pre>
     */
    static String buildArrayContainsAll(String joinKey, String filterKey, String paramName, Object paramValue, List<CosmosSqlParameter> params) {

        Checker.checkNotBlank(joinKey, "joinKey");
        Checker.checkNotNull(filterKey, "filterKey");
        Checker.checkNotBlank(paramName, "paramName");

        if (paramValue instanceof Collection<?>) {

            if (ObjectUtils.isEmpty(paramValue)) {
                return "(1=0)";
            }
            var paramCollection = (Collection<?>) paramValue;

            var index = 0;
            var subQueries = new ArrayList<String>();

            for (var value : paramCollection) {
                var subParamName = String.format("%s__%d", paramName, index);
                params.add(Condition.createSqlParameter(subParamName, value));
                subQueries.add(buildSimpleSubQuery(joinKey, filterKey, subParamName));
                index++;
            }

            // AND all subQueies
            return String.format(" (%s)", String.join(" AND ", subQueries));

		} else {
			//scalar
			params.add(Condition.createSqlParameter(paramName, paramValue));
			return String.format( " (%s)", buildSimpleSubQuery(joinKey, filterKey, paramName));
		}
	}

	static String buildSimpleSubQuery(String joinKey, String filterKey, String paramName) {
		return String.format("EXISTS(SELECT VALUE x FROM x IN %s WHERE %s = %s)",
				Condition.getFormattedKey(joinKey), Condition.getFormattedKey(filterKey, "x"), paramName);
	}


}
