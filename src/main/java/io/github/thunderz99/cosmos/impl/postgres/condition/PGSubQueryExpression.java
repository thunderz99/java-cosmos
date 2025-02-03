package io.github.thunderz99.cosmos.impl.postgres.condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * a class representing expression using subqueries and array operations(ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL)
 *
 * {@code
 * ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL
 * }
 *
 * <p>
 *     see postgres JSONB array operators for details
 * </p>
 */
public class PGSubQueryExpression implements Expression {

	public static final String ARRAY_CONTAINS_ANY = "ARRAY_CONTAINS_ANY";
	public static final String ARRAY_CONTAINS_ALL = "ARRAY_CONTAINS_ALL";

	/**
	 * the key for JOIN for subquery. e.g. "items" part for data->'items'
	 */
	public String joinKey;

	/**
	 * the key for filter for subquery. e.g. "id" part for data->'items'->'id'. Maybe empty if c.items is List of string / number.
	 */
	public String filterKey = "";
	public Object value;
	public String operator = "=";

    public PGSubQueryExpression() {
    }

    public PGSubQueryExpression(String joinKey, String filterKey, Object value, String operator) {
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
            queryText = buildArrayContainsAll(this.joinKey, this.filterKey, paramName, paramValue, params, selectAlias);
        } else {
            queryText = buildArrayContainsAny(this.joinKey, this.filterKey, paramName, paramValue, params, selectAlias);
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
     * A helper function to generate data->'items' ARRAY_CONTAINS_ANY List.of(item1, item2) queryText
     *
     * <pre>
     * INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
     * OUTPUT:
     * " data->'items' ??| @items_009"
     *
     *
     * INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
     * OUTPUT:
     * "  (data->'items' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__0)
     * OR data->'items' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__1)
     * OR data->'items' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__2)
     * "
     *
     *  and add paramsValue into params
     * </pre>
     *
     * @param joinKey e.g. items
     * @param filterKey e.g. id
     * @param paramName e.g. @items_009
     * @param _paramValue e.g. ["id001", "id002", "id005"]
     * @param params all params in order to build the querySpec
     * @param selectAlias e.g. "j0" "s0" for cond.join
     * @return subQuery text for ARRAY_CONTAINS_ANY
     */
    static String buildArrayContainsAny(String joinKey, String filterKey, String paramName, Object _paramValue, List<CosmosSqlParameter> params, String selectAlias) {

        Checker.checkNotBlank(joinKey, "joinKey");
        Checker.checkNotNull(filterKey, "filterKey");
        Checker.checkNotBlank(paramName, "paramName");

        Collection<?> paramValue;

        // turn paramValue into a collection if it is a scalar value
        if (_paramValue instanceof Collection<?> collValue) {
            paramValue = collValue;
        } else {
            paramValue = List.of(_paramValue);
        }

        if (CollectionUtils.isEmpty(paramValue)) {
            // if value is empty, return false condition
            return "(1=0)";
        }

        /** not work for integers, comment out
        if(StringUtils.isEmpty(filterKey)){

            // paramValue contains only 1 element, do a ARRAY_CONTAINS 1 element query
            if(paramValue.size() == 1){
                // Condition.filter("items ARRAY_CONTAINS_ANY", List.of("A"))
                // INPUT: "items", "", "@items_009", ["id001"], params
                // OUTPUT: " data->'items' ?? @items_009"
                var singleValue = paramValue.iterator().next();
                params.add(Condition.createSqlParameter(paramName, String.valueOf(singleValue)));
                var format = (singleValue instanceof String) ? " (%s @> \"%s\"::jsonb)" : " (%s @> %s::jsonb)";
                return String.format(format,
                        PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), paramName);
            }

            // paramValue is a collection with multiple elements

            // Condition.filter("items ARRAY_CONTAINS_ANY", List.of("A","B"))
            // INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
            // OUTPUT: " data->'items' @> @items_009::jsonb"
            // ??| only works for string arrays
            params.add(Condition.createSqlParameter(paramName, JsonUtil.toJson(paramValue)));
            return String.format(" (%s @> %s::jsonb)",
                    PGKeyUtil.getFormattedKeyWithAlias(joinKey, selectAlias, paramValue), paramName);
        }
         */

        // Condition.filter("items ARRAY_CONTAINS_ANY id", List.of("A","B"))
        // INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
        // OUTPUT:
        // data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_id_010__0)
        // OR data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_id_010__1)

        var index = 0;
        var subQueries = new ArrayList<String>();


        for(var value : paramValue){
            // break down the list values to multiple single value
            var subParamName = String.format("%s__%d", paramName, index);

            if(StringUtils.isEmpty(filterKey)) {
                //simple key
                if(value instanceof Integer) {
                    //for integer value
                    params.add(Condition.createSqlParameter(subParamName, String.valueOf(value)));
                } else {
                    //for str value
                    params.add(Condition.createSqlParameter(subParamName, "\"%s\"".formatted(value)));
                }
                subQueries.add(String.format("%s @> %s::jsonb", PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), subParamName));
            } else {
                //nested key
                params.add(Condition.createSqlParameter(subParamName, value));
                subQueries.add(String.format("%s @> %s", PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), buildNestedJsonbExpression(filterKey, subParamName)));
            }


            index++;
        }

        return String.format(" (%s)", String.join(" OR ", subQueries));

    }

    /**
     *
     *  Builds a postgres JSONB expression for nested keys.
     *  Example: For input "school.grade", "@param000_grade", returns:
     *  "jsonb_build_array(jsonb_build_object('school', jsonb_build_object('grade', @param000_grade)))"
     *
     * @param key
     * @param paramName
     * @return
     */
    static String buildNestedJsonbExpression(String key, String paramName) {

        Checker.checkNotBlank(key, "key");
        Checker.checkNotBlank(paramName, "paramName");

        var keyParts = key.split("\\.");

        Checker.checkNotEmpty(keyParts, "parts");

        // Build the innermost jsonb_build_object first
        var expression = new StringBuilder();
        expression.append("jsonb_build_object('")
                .append(keyParts[keyParts.length - 1])
                .append("', %s)".formatted(paramName));

        // Wrap with outer jsonb_build_object calls
        for (int i = keyParts.length - 2; i >= 0; i--) {
            expression.insert(0, "jsonb_build_object('" + keyParts[i] + "', ");
            expression.append(")");
        }

        // Wrap the final result in jsonb_build_array
        return "jsonb_build_array(" + expression + ")";

    }


    /**
     * A helper function to generate data->'items' ARRAY_CONTAINS_ALL List.of(item1, item2) queryText
     *
     * <pre>
     * INPUT: "items", "", "@items_009", ["id001", "id002"], params
     * OUTPUT:
     * " data->'items' @> '[\"id001\",\"id002\"]'::jsonb
     *
     *
     * INPUT: "tags", "name", "@tags_name_010", ["react", "java"], params
     * OUTPUT:
     * "  (data->'tags' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__0)
     * AND data->'tags' @> jsonb_build_array(jsonb_build_object('name', @tags_name_010__1)
     *
     *  and add paramsValue into params
     * </pre>
     *
     * @param joinKey e.g. items
     * @param filterKey e.g. id
     * @param paramName e.g. @items_009
     * @param _paramValue e.g. ["id001", "id002"]
     * @param params all params in order to build the querySpec
     * @param selectAlias e.g. "j0" "s0" for cond.join
     * @return subQuery text for ARRAY_CONTAINS_ALL
     */
    static String buildArrayContainsAll(String joinKey, String filterKey, String paramName, Object _paramValue, List<CosmosSqlParameter> params, String selectAlias) {

        Checker.checkNotBlank(joinKey, "joinKey");
        Checker.checkNotNull(filterKey, "filterKey");
        Checker.checkNotBlank(paramName, "paramName");

        Collection<?> paramValue;

        // turn paramValue into a collection if it is a scalar value
        if (_paramValue instanceof Collection<?> collValue) {
            paramValue = collValue;
        } else {
            paramValue = List.of(_paramValue);
        }

        if (CollectionUtils.isEmpty(paramValue)) {
            // if value is empty, return false condition
            return "(1=0)";
        }

        /** not work for integers, comment out
        if(StringUtils.isEmpty(filterKey)){

            // paramValue contains only 1 element, do a ARRAY_CONTAINS 1 element query
            if(paramValue.size() == 1){
                // Condition.filter("items ARRAY_CONTAINS_ALL", List.of("A"))
                // INPUT: "items", "", "@items_009", ["id001"], params
                // OUTPUT: " data->'items' ?? @items_009"
                var singleValue = paramValue.iterator().next();
                params.add(Condition.createSqlParameter(paramName, singleValue));
                return String.format(" (%s ?? %s)",
                        PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), paramName);
            }

            // Condition.filter("items ARRAY_CONTAINS_ALL", List.of("A","B"))
            // INPUT: "items", "", "@items_009", ["id001", "id002", "id005"], params
            // OUTPUT: " data->'items' @> @items_009::jsonb"
            params.add(Condition.createSqlParameter(paramName, JsonUtil.toJson(paramValue)));

            return String.format(" (%s @> %s::jsonb)",
                    PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), paramName);
        }
         */


        // for complicated pattern with filterKey

        // Condition.filter("items ARRAY_CONTAINS_ALL id", List.of("A","B"))
        // INPUT: "items", "id", "@items_id_010", ["id001", "id002", "id005"], params
        // OUTPUT: "  (data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_id_010__0)
        // AND data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_id_010__1)
        // AND data->'items' @> jsonb_build_array(jsonb_build_object('name', @items_id_010__2)

        var index = 0;
        var subQueries = new ArrayList<String>();


        for(var value : paramValue){
            // break down the list values to multiple single value
            var subParamName = String.format("%s__%d", paramName, index);

            if(StringUtils.isEmpty(filterKey)) {
                //simple key
                if(value instanceof Integer) {
                    //for integer value
                    params.add(Condition.createSqlParameter(subParamName, String.valueOf(value)));
                } else {
                    //for str value
                    params.add(Condition.createSqlParameter(subParamName, "\"%s\"".formatted(value)));
                }
                subQueries.add(String.format("%s @> %s::jsonb", PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), subParamName));
            } else {
                //nested key
                params.add(Condition.createSqlParameter(subParamName, value));
                subQueries.add(String.format("%s @> %s", PGKeyUtil.getFormattedKey4JsonWithAlias(joinKey, selectAlias), buildNestedJsonbExpression(filterKey, subParamName)));
            }

            index++;
        }

        return String.format(" (%s)", String.join(" AND ", subQueries));

	}


}
