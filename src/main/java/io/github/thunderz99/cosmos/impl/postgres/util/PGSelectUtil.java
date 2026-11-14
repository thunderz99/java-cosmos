package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.FilterQueryInfo4Join;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.impl.postgres.dto.SelectClause;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * A util class to generate SELECT clause, and save FilterQueryInfo and generate SELECT clause for complicated join query
 */
public class PGSelectUtil {

    /**
     * Maximum number of key/value pairs in one jsonb_build_object call.
     * PostgreSQL's default limit is 100 function arguments, and each pair consumes two.
     */
    static final int MAX_JSON_BUILD_OBJECT_PAIRS = 50;

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
        var select= generateSelect(cond, QueryContext.create(), List.of());
        return select.selectPart;
    }


    /**
     * Generate select considering join and returnAllSubArray=false
     *
     * {@code
     *  e.g.
     *  "id", "age", "fullName.first" -> VALUE {"id":c.id, "age":c.age, "fullName": {"first": c.fullName.first}}
     *  }
     *
     * @param cond
     * @param queryContext
     * @param params
     * @return
     */
    static SelectClause generateSelect(Condition cond, QueryContext queryContext, List<CosmosSqlParameter> params) {

        // whether we should filter select parts' subArrays
        // see docs/postgres-find-with-join.md for details
        var shouldFilterSelectParts = cond.returnAllSubArray == false
                && CollectionUtils.isNotEmpty(cond.join)
                && MapUtils.isNotEmpty(queryContext.filterQueryInfos4Join);

        if (!shouldFilterSelectParts) {
            // normal select
            if (CollectionUtils.isEmpty(cond.fields)) {
                return new SelectClause("*");
            }
            var selectPart = generateSelectByFields(cond.fields);
            return new SelectClause(selectPart, "");
        } else {

            // for join and returnAllSubArray == false
            // see docs/postgres-find-with-join.md for details

            if (CollectionUtils.isEmpty(cond.fields)) {
                var selectPart = generateSelectToFilteringSubArray(queryContext.filterQueryInfos4Join, params);
                return new SelectClause(selectPart, "");
            } else {
                // with fields
                var selectPart = generateSelectToFilteringSubArray(queryContext.filterQueryInfos4Join, params);
                var projectionPart = generateSelectByFields(cond.fields);

                /**
                 * WITH filtered_data AS MATERIALIZED (
                 * %s
                 * )
                 * SELECT
                 *   projectionPart
                 * FROM filtered_data
                 */
                var withClause = new StringBuilder();
                // Calculate the filtered JSON separately so each projected field can reuse it.
                // Inlining the JSON reconstruction and its subqueries into thousands of field
                // references can exhaust backend memory. Apply this to every filtered JOIN
                // projection, since the 50-key function limit is not a safe OOM threshold.
                withClause.append("WITH filtered_data AS MATERIALIZED (\n");
                withClause.append("%s\n");
                withClause.append(")\n");
                withClause.append("SELECT\n");
                withClause.append(projectionPart);
                withClause.append("\nFROM filtered_data");

                return new SelectClause(selectPart, withClause.toString());
            }
        }
    }


    /**
     *
     * generate select part for filtering subArray
     *
     * e.g. the select part of the following query
     *
     * see docs/postgres-find-with-join.md for details
     *
     * @code{
     *
     * // return
     *     id,
     *     jsonb_set(
     *       jsonb_set(
     *         data,
     *         '{area,city,street,rooms}',
     *         (
     *           SELECT jsonb_agg(s0)
     *           FROM jsonb_array_elements(data->'area'->'city'->'street'->'rooms') s0
     *           WHERE (s0->>'no' = @param000_no__for_select)
     *         )
     *       ),
     *       '{room*no-01}',
     *       (
     *         SELECT jsonb_agg(s1)
     *         FROM jsonb_array_elements(data->'room*no-01') s1
     *         WHERE ((s1->>'area')::int > @param001_area__for_select)
     *       )
     *     ) AS data
     *
     *
     * @param subQueryMap
     * @return
     */
    static String generateSelectToFilteringSubArray(Map<String, List<FilterQueryInfo4Join>> subQueryMap, List<CosmosSqlParameter> params) {

        var selectPart = TableUtil.DATA;

        var ret = new StringBuilder();

        for(var baseEntry : subQueryMap.entrySet()) {
            var baseKey = baseEntry.getKey();

            // '{area,city,street,rooms}'
            // or '{"floors"}'
            var pathLiteral = toPathLiteral(baseEntry.getKey());

            var jsonbSetClause = """
                        jsonb_set(
                          %s,
                          '%s',
                          COALESCE(
                            %s,
                            %s
                          )
                        )
                        """;

            var filterClause = """
                         (
                           SELECT jsonb_agg(%s)
                           FROM jsonb_array_elements(%s) AS %s
                           WHERE %s
                         )
                        """;

            var filterQueryList = baseEntry.getValue();

            if(CollectionUtils.isEmpty(filterQueryList)) {
                continue;
            }

            var firstFilterQueryInfo = filterQueryList.get(0);

            var alias4Select = "s" + firstFilterQueryInfo.paramIndex;

            var whereSubQueryList = new ArrayList<String>();

            for (var filterQueryInfo : filterQueryList) {

                var subExp = filterQueryInfo.subExp;
                var paramIndex = filterQueryInfo.paramIndex;

                var subQuery = subExp.toQuerySpec(paramIndex, alias4Select);

                filterClause = StringUtils.removeEnd(filterClause, "\n");

                // for where subQuery
                var subQueryText = subQuery.queryText;

                // change the param names for select, in order to avoid param name conflict

                var params4Select = new ArrayList<CosmosSqlParameter>();

                for(var param : subQuery.getParameters()) {

                    var paramName = param.getName();
                    String newParamName = paramName + "__for_select";

                    var newParam = new CosmosSqlParameter(newParamName, param.getValue());
                    params4Select.add(newParam);
                    // Match a complete placeholder: __1 must not rename __10 in large ANY lists.
                    subQueryText = subQueryText.replaceAll(Pattern.quote(paramName) + "(?![A-Za-z0-9_])",
                            Matcher.quoteReplacement(newParamName));
                }

                whereSubQueryList.add(subQueryText);
                params.addAll(params4Select);

                // end loop for where subQuery

            }

            //end loop for filter clause part
            var whereSubQueries = whereSubQueryList.stream().collect(Collectors.joining("\n   AND", "(", ")"));

            var formattedBaseKey = PGKeyUtil.getFormattedKey4JsonWithAlias(baseKey, TableUtil.DATA);
            filterClause = filterClause.formatted(alias4Select, formattedBaseKey, alias4Select, whereSubQueries);


            selectPart = String.format(jsonbSetClause, selectPart, pathLiteral, filterClause, formattedBaseKey);
        }

        return "id, %s AS %s".formatted(selectPart, TableUtil.DATA);

    }

    /**
     * build a postgres text-array literal like '{"area","city","street","rooms"}', from "area.city.street.rooms"
     */
    static String toPathLiteral(String key) {

        if(StringUtils.isEmpty(key)){
            return key;
        }
        return  Arrays.stream(key.split("\\."))
                .filter(StringUtils::isNotEmpty)
                .map(k -> StringUtils.startsWith(k,"\"") ? k : "\"" + k + "\"")
                .collect(Collectors.joining(",", "{", "}"));

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
     * @return select part. e.g. "id, jsonb_build_object..."
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

        // add "id" fields as a must
        fields.add(TableUtil.ID);

        // Separate top-level fields (no dot) from nested fields
        var topLevel = new LinkedHashSet<String>(); // should contain "id" only

        // should be like ["id", "name", "address.city"]
        // note that the "data." part is removed for the simplicity of the following process
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
     * Recursively builds a JSONB projection while respecting the function argument limit.
     * Each node is checked independently: its child count, rather than the total number
     * of requested fields, determines whether its object must be split into groups.
     *
     * @param pathSoFar SQL expression referring to this node, e.g. {@code data->'address'}
     * @param node projection subtree whose children are unique keys at this level
     * @return the leaf expression, a single object, or a parenthesized merge of objects
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

        // Preserve the existing SQL text for nodes that fit in one function call.
        if (pairs.size() <= MAX_JSON_BUILD_OBJECT_PAIRS) {
            return "jsonb_build_object(" + String.join(", ", pairs) + ")";
        }

        // Split complete key/value pairs so no function receives more than 100 arguments.
        // Child expressions have already been built recursively, including any deeper splits.
        var objects = new ArrayList<String>();
        for (int start = 0; start < pairs.size(); start += MAX_JSON_BUILD_OBJECT_PAIRS) {
            var end = Math.min(start + MAX_JSON_BUILD_OBJECT_PAIRS, pairs.size());
            objects.add("jsonb_build_object(" + String.join(", ", pairs.subList(start, end)) + ")");
        }
        // Sibling keys are unique, so shallow JSONB concatenation cannot overwrite another group.
        // Parentheses keep the entire merge together when nested or followed by a cast/operator.
        return "(" + String.join(" || ", objects) + ")";
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

    /**
     * save the query information for complicated SELECT clause using join and returnAllSubArray=false
     *
     * @param queryContext the context to save the information
     * @param baseKey      the base key, e.g. "floors"
     * @param paramIndex  the param index
     * @param subExp      the sub expression
     */
    public static void saveQueryInfo4Join(QueryContext queryContext, String baseKey, AtomicInteger paramIndex, Expression subExp) {

        var queryList = queryContext.filterQueryInfos4Join.get(baseKey);
        if (queryList == null) {
            queryList = new ArrayList<>();
        }
        queryList.add(new FilterQueryInfo4Join(baseKey, paramIndex, subExp));
        queryContext.filterQueryInfos4Join.put(baseKey, queryList);

    }

}

