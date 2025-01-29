package io.github.thunderz99.cosmos.impl.postgres.condition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.PGFilterOptions;
import io.github.thunderz99.cosmos.impl.postgres.util.PGKeyUtil;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.JsonUtil;
import io.github.thunderz99.cosmos.util.ParamUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * a class representing expression using subqueries and array operations(ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL), used in json path query
 *
 * {@code
 * ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL
 * }
 *
 * <p>
 *     see postgres json path query for details
 * </p>
 */
public class PGSubQueryExpression4JsonPath implements Expression {

	public static final String ARRAY_CONTAINS_ANY = "ARRAY_CONTAINS_ANY";
	public static final String ARRAY_CONTAINS_ALL = "ARRAY_CONTAINS_ALL";

	/**
	 * the key for JOIN for subquery. e.g. "items" part for data->'items'
	 */
	public String joinKey;

	/**
	 * the key for filter for subquery. e.g. "id" part for data->'items'->'id'. Maybe empty if items is List of string / number.
	 */
	public String filterKey = "";
	public Object value;
	public String operator = "=";
    public PGFilterOptions filterOptions;

    public PGSubQueryExpression4JsonPath() {
    }

    public PGSubQueryExpression4JsonPath(String joinKey, String filterKey, Object value, String operator, PGFilterOptions filterOptions) {
        this.joinKey = joinKey;
        this.filterKey = filterKey;
        this.value = value;
        this.operator = operator;

        // for jsonPath expression, the filterOptions.join must not be empty
        Checker.checkNotNull(filterOptions, "filterOptions");
        Checker.checkNotEmpty(filterOptions.join, "filterOptions");

        this.filterOptions = filterOptions;
    }

    @Override
    public CosmosSqlQuerySpec toQuerySpec(AtomicInteger paramIndex, String selectAlias) {

        var baseKey = ""; // e.g. "floors"
        for(var subKey : this.filterOptions.join) {
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


        var ret = new CosmosSqlQuerySpec();
        var params = new ArrayList<CosmosSqlParameter>();

        // joinKey.filterKey : fullName.last -> @param001_fullName__last
        var key = List.of(this.joinKey, this.filterKey).stream().filter(StringUtils::isNotEmpty).collect(Collectors.joining("."));
        var paramName = ParamUtil.getParamNameFromKey(key, paramIndex.get());
        var paramValue = this.value;

        paramIndex.getAndIncrement();

        var queryText = "";
        if (ARRAY_CONTAINS_ALL.equals(this.operator)) {
            queryText = buildArrayContainsAll(baseKey, this.joinKey, this.filterKey, paramName, paramValue, params);
        } else {
            queryText = buildArrayContainsAny(baseKey, this.joinKey, this.filterKey, paramName, paramValue, params);
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
     * INPUT: "rooms.no", "", "@rooms_no_009", ["001", "002"], params, join:"rooms"
     * OUTPUT:
     *  data @? '$.rooms[*].no[*] ? (@ == "001" || @ == "002")'::jsonpath;
     *
     *
     * INPUT: "floors.rooms", "name", "@floors_rooms_name_010", ["001", "002"], params
     * OUTPUT:
     *  data @? '$.floors[*].rooms[*] ? (@.name == "r1" || @.name == "r2")'::jsonpath;
     *
     *  and add paramsValue into params
     * </pre>
     *
     * @@aram baseKey e.g. floors
     * @param joinKey e.g. rooms
     * @param filterKey e.g. name
     * @param paramName e.g. @floors_rooms_name_009
     * @param _paramValue e.g. ["001", "002"]
     * @param params all params in order to build the querySpec
     * @return subQuery text for ARRAY_CONTAINS_ANY
     */
    static String buildArrayContainsAny(String baseKey, String joinKey, String filterKey, String paramName, Object _paramValue, List<CosmosSqlParameter> params) {

        Checker.checkNotBlank(baseKey, "baseKey");
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


        // for complicated pattern with filterKey

        /**
         * SELECT * FROM schema1.table1
         * WHERE
         * data @? '$."floors"[*]."rooms"[*] ? (@."name" == "r1" || @."name" == "r2")'::jsonpath
         */

        // subQueryText is simple. e.g. "data @? @param000_floors_rooms_name::jsonpath"
        var queryText = " (data @?? %s::jsonpath)".formatted(paramName);

        // the value is complicated e.g. $."floors"[*]."rooms"[*] ? (@."name" == "r1" || @."name" == "r2")

        // pathMatching in value. e.g. $."floors"[*]."rooms"[*]
        var pathMatching = Arrays.asList(joinKey.split("\\.")).stream().map( k -> "\"%s\"[*]".formatted(k))
                .collect(Collectors.joining(".", "$.", " ?"));

        // next, we build the filter part: (@."name" == "r1" || @."name" == "r2")

        var filterKeys = Lists.newArrayList("@");
        filterKeys.addAll(Arrays.asList(filterKey.split("\\.")));
        // filterKey -> formattedFilterKey. e.g. "name" -> "@.\"name\""
        var formattedFilterKey = filterKeys.stream()
                .filter(StringUtils::isNotEmpty)
                .map(k -> k.equals("@") ? "@" : "\"%s\"".formatted(k))
                .collect(Collectors.joining("."));

        var filterPart = paramValue.stream()
                .map(v -> (v instanceof String strValue) ? "\"%s\"".formatted(strValue) : v)
                .map(v -> "%s == %s".formatted(formattedFilterKey, v))
                .collect(Collectors.joining(" || ", "(", ")"));

        var filterValue = "%s %s".formatted(pathMatching, filterPart);
        params.add(new CosmosSqlParameter(paramName, filterValue));

        return queryText;

    }

    /**
     * A helper function to generate data->'items' ARRAY_CONTAINS_ALL List.of(item1, item2) queryText
     *
     * <pre>
     * INPUT: "rooms.no", "", "@rooms_no_009", ["001", "002"], params, join:"rooms"
     * OUTPUT:
     *  data @? '$.rooms[*] ? (@.no[*] ? (@ == "001") && @.no[*] ? ( @ == "002"))'::jsonpath;
     *
     *
     * INPUT: "floors.rooms", "name", "@floors_rooms_name_010", ["001", "002"], params
     * OUTPUT:
     *  data @? '$.floors[*] ? (@.rooms[*] ? (@.name == "r1") && @.rooms[*] ? (@.name == "r2"))'::jsonpath;
     *
     *  and add paramsValue into params
     * </pre>
     *
     * @@aram baseKey e.g. floors
     * @param joinKey e.g. rooms
     * @param filterKey e.g. name
     * @param paramName e.g. @floors_rooms_name_009
     * @param _paramValue e.g. ["001", "002"]
     * @param params all params in order to build the querySpec
     * @return subQuery text for ARRAY_CONTAINS_ANY
     */
    static String buildArrayContainsAll(String baseKey, String joinKey, String filterKey, String paramName, Object _paramValue, List<CosmosSqlParameter> params) {

        Checker.checkNotBlank(baseKey, "baseKey");
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


        // for complicated pattern with filterKey

        /**
         * SELECT * FROM schema1.table1
         * WHERE data @? '
         *   $.floors[*] ? (
         *     @."rooms"[*] ? (@."name" == "r1")
         *     &&
         *     @."rooms"[*] ? (@."name" == "r2")
         *   )
         * '
         */

        // subQueryText is simple. e.g. "data @? @param000_floors_rooms_name::jsonpath"
        var queryText = " (data @?? %s::jsonpath)".formatted(paramName);

        // the value is complicated e.g. $."floors"[*] ? (@.rooms[*] ? (@.name == "r1") && @.rooms[*] ? (@.name == "r2"))

        // step1, basePart in value. e.g. $."floors"[*]
        var basePart = Arrays.asList(baseKey.split("\\.")).stream().map( k -> "\"%s\"[*]".formatted(k))
                .collect(Collectors.joining(".", "$.", ""));

        // step2, we build the join part: @.rooms[*]

        // floors.rooms -> rooms
        var joinSubKey = StringUtils.removeStart(joinKey, baseKey + ".");
        var joinKeys = Lists.newArrayList("@");
        joinKeys.addAll(Arrays.asList((joinSubKey).split("\\.")));
        var joinPart = joinKeys.stream()
                .map( k -> "@".equals(k)? "@" :"\"%s\"".formatted(k))
                .collect(Collectors.joining(".", "", "[*]"));

        // step3, we build the filter part: (exists(@.rooms[*] ? (@.name == "r1")) && exists(@.rooms[*] ? (@.name == "r2"))

        var filterKeys = Lists.newArrayList("@");
        filterKeys.addAll(Arrays.asList(filterKey.split("\\.")));

        // filterKey -> formattedFilterKey. e.g. "name" -> "\"name\""
        var formattedFilterKey = filterKeys.stream()
                .filter(StringUtils::isNotEmpty)
                .map(k -> "@".equals(k)? "@" : "\"%s\"".formatted(k))
                .collect(Collectors.joining("."));

        var filterPart = paramValue.stream()
                .map(v -> (v instanceof String strValue) ? "\"%s\"".formatted(strValue) : v)
                .map(v -> "exists(%s ? (%s == %s))".formatted(joinPart, formattedFilterKey, v))
                .collect(Collectors.joining(" && ", "(", ")"));

        var filterValue = "%s ? %s".formatted(basePart, filterPart);
        params.add(new CosmosSqlParameter(paramName, filterValue));

        return queryText;

    }


}
