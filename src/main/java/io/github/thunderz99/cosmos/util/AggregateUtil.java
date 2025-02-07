package io.github.thunderz99.cosmos.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import io.github.thunderz99.cosmos.condition.Aggregate;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * A util class that generate bson data for aggregate pipeline
 */
public class AggregateUtil {

    public static final String REGEX_AS = "(?i)\\s+AS\\s+";

    private static final Pattern FUNCTION_PATTERN = Pattern.compile("^(?i)(?:SUM|AVG|MIN|MAX|COUNT|ARRAY_LENGTH)\\(([^)]+)\\)$");

    /**
     * Project fields with renamed keys if key name contains dot "."
     *
     * @param aggregate
     * @return bson for project stage alias field include dot "."
     */
    public static Bson createProjectStage(Aggregate aggregate) {
        var projection = new Document();

        var projectionSet = new HashSet<String>();
        // Project fields with renamed keys if necessary
        for (var groupByField : aggregate.groupBy) {
            var dotFieldName = FieldNameUtil.convertToDotFieldName(groupByField);
            var fieldInPipeline = convertFieldNameIncludingDot(dotFieldName);

            if (!projectionSet.contains(fieldInPipeline)) {
                // add the alias and field only if not added already
                projection.append(fieldInPipeline, "$" + dotFieldName);
                projectionSet.add(fieldInPipeline);
            }
        }

        // Include all fields that will be used in aggregate functions
        var functionParts = aggregate.function.split(",");
        for (var functionPart : functionParts) {
            var function = extractFunctionAndAlias(functionPart).getLeft();

            if (StringUtils.startsWithIgnoreCase(function, "COUNT(")) {
                //count do not need a field "COUNT(1)"
                continue;
            }

            var field = extractFieldFromFunction(function);

            var dotFieldName = FieldNameUtil.convertToDotFieldName(field);
            var fieldInPipeline = convertFieldNameIncludingDot(dotFieldName);
            if (!projectionSet.contains(fieldInPipeline)) {
                // add the alias and field only if not added already
                projection.append(fieldInPipeline, "$" + dotFieldName);
                projectionSet.add(fieldInPipeline);
            }
        }

        if (!projection.toBsonDocument().isEmpty()) {
            return Aggregates.project(projection);
        }

        // empty projection
        return projection;
    }

    /**
     * Replace dot "." to "__" because aggregate pipeline does not allow field including "."
     *
     * @param field
     * @return field name without dot
     */
    public static String convertFieldNameIncludingDot(String field) {
        if (StringUtils.isEmpty(field)) {
            return field;
        }

        if (field.contains(".")) {
            return field.replace(".", "__");
        }

        return field;
    }

    /**
     * Generate group stage of mongo aggregate pipeline from a Aggregate dto input
     *
     * @param aggregate
     * @return group stage in bson
     */
    public static List<Bson> createGroupStage(Aggregate aggregate) {
        if (aggregate.function.isEmpty()) {
            return null;
        }

        // accumulators for MIN/MAX/AVG/SUM/COUNT
        var accumulators = new ArrayList<BsonField>();

        // preProjections for SUM(ARRAY_LENGTH(xxx))
        var preFieldProjections = new Document();

        var functionParts = aggregate.function.split(",");

        // Add accumulators for each aggregate function
        for (var functionPart : functionParts) {
            var functionAndAlias = extractFunctionAndAlias(functionPart);

            var function = functionAndAlias.getLeft();
            var alias = functionAndAlias.getRight();

            if (StringUtils.startsWithIgnoreCase(function, "COUNT(")) {
                accumulators.add(Accumulators.sum(alias, 1));
            } else if (StringUtils.startsWithIgnoreCase(function, "SUM(ARRAY_LENGTH(")) {

                var field = extractFieldFromFunction(function);

                var dotFieldName = FieldNameUtil.convertToDotFieldName(field);
                var fieldInPipeline = convertFieldNameIncludingDot(dotFieldName);

                // array_length projection is always before group and accumulators
                var fieldInPipeline4ArrayLength = fieldInPipeline + "__array_length";
                preFieldProjections.append(fieldInPipeline4ArrayLength, createArrayLengthProjection(fieldInPipeline));
                accumulators.add(Accumulators.sum(alias, "$" + fieldInPipeline4ArrayLength));

            } else {
                var field = extractFieldFromFunction(function);

                var dotFieldName = FieldNameUtil.convertToDotFieldName(field);
                var fieldInPipeline = convertFieldNameIncludingDot(dotFieldName);

                if (StringUtils.startsWithIgnoreCase(function, "MAX")) {
                    accumulators.add(Accumulators.max(alias, "$" + fieldInPipeline));
                } else if (StringUtils.startsWithIgnoreCase(function, "MIN")) {
                    accumulators.add(Accumulators.min(alias, "$" + fieldInPipeline));
                } else if (StringUtils.startsWithIgnoreCase(function, "SUM")) {
                    accumulators.add(Accumulators.sum(alias, "$" + fieldInPipeline));
                } else if (StringUtils.startsWithIgnoreCase(function, "AVG")) {
                    accumulators.add(Accumulators.avg(alias, "$" + fieldInPipeline));
                } else if (StringUtils.startsWithIgnoreCase(function, "SUM")) {
                    accumulators.add(Accumulators.sum(alias, "$" + fieldInPipeline));
                } else {
                    // simple field. like: c.address.state AS result
                    // $project: { result : $address__state }
                    preFieldProjections.append(alias, "$" + fieldInPipeline);
                }
            }

        }

        // Create the group key based on the groupBy fields (use renamed fields where necessary)
        Document groupId;

        if (CollectionUtils.isEmpty(aggregate.groupBy)) {
            // mongo db allows groupId = null when there is no groupBy
            groupId = null;
        } else {
            groupId = new Document();
            for (var groupByField : aggregate.groupBy) {
                var fieldInPipeline = convertFieldNameIncludingDot(groupByField);
                groupId.append(fieldInPipeline, "$" + fieldInPipeline);

            }
        }

        var subPipelines = new ArrayList<Bson>();

        if (!preFieldProjections.isEmpty()) {
            subPipelines.add(Aggregates.project(preFieldProjections));
        }

        if (!accumulators.isEmpty()) {
            // Create the group stage
            subPipelines.add(Aggregates.group(groupId, accumulators));
        }

        return subPipelines;
    }

    /**
     * extract field name from SUM(ARRAY_LENGTH(c.address.city.street)) to "c.address.city.street"
     *
     * @param function
     * @return
     */
    public static String extractFieldFromFunction(String function) {

        if (StringUtils.isEmpty(function)) {
            return function;
        }

        if (StringUtils.startsWithIgnoreCase(function, "SUM(ARRAY_LENGTH(")) {
            // this a special case at present. TODO: generalize this
            return function.substring(function.indexOf("ARRAY_LENGTH(") + 13, function.lastIndexOf("))")).trim();
        }

        var matcher = FUNCTION_PATTERN.matcher(function);
        if (matcher.find()) {
            // Return the first capturing group, which is the content inside the parentheses
            return matcher.group(1).trim();
        }

        // If no match, return the original string
        return function.trim();
    }

    /**
     * Add projection stage for ARRAY_LENGTH(c.area.city.street.rooms)
     *
     * @param fieldInPipeline "area__city__street__rooms"
     * @return value part of projection
     */
    static Document createArrayLengthProjection(String fieldInPipeline) {
        /*
          {
            $project:
            { // this value part will be returned
              area__city__street__rooms__array_length: { $size: { $ifNull: ["$area__city__street__rooms", []] } }  // Get the length of the children array, or 0 if it's null
            }
          },
         */

        return new Document("$size", new Document("$ifNull", List.of("$" + fieldInPipeline, List.of())));

    }


    /**
     * Convert mongo aggregate result to cosmos aggregate result
     *
     * <pre>
     * e.g.
     * from:
     * ```
     * [ {
     *   "_id" : {
     *     "fullName_last" : "Hanks"
     *   },
     *   "facetCount" : 2
     * }, {
     *   "_id" : {
     *     "fullName_last" : "Henry"
     *   },
     *   "facetCount" : 1
     * } ]
     * ```
     *
     * to:
     * ```
     * [ {
     *   "facetCount" : 1,
     *   "last" : "Henry"
     * }, {
     *   "facetCount" : 2,
     *   "last" : "Hanks"
     * } ]
     * ```
     *
     *
     * </pre>
     * @param aggregate
     * @return bson for final project stage
     */
    public static Bson createFinalProjectStage(Aggregate aggregate) {
        var projection = new Document();

        // Extract fields from the _id and rename them
        for (var groupByField : aggregate.groupBy) {
            var fieldInPipeline = FieldNameUtil.convertToDotFieldName(groupByField);
            fieldInPipeline = convertFieldNameIncludingDot(groupByField);
            var finalFieldName = getSimpleName(fieldInPipeline);
            projection.append(finalFieldName, "$_id." + fieldInPipeline);
        }

        // Include all calculated fields (e.g., facetCount, maxAge) directly from the group stage
        var functionParts = aggregate.function.split(",");
        for (var functionPart : functionParts) {

            var functionAndAlias = extractFunctionAndAlias(functionPart);
            var function = functionAndAlias.getLeft();
            var alias = functionAndAlias.getRight();

            if (StringUtils.isNotEmpty(alias)) {
                // `max(c.age) AS maxAge` will be maxAge
                projection.append(alias, 1);
            } else {
                // `max(c.age)` without AS will be $1 or $2, etc. According to cosmosdb's spec
                // TODO $1 and $2 not implemented
                alias = function;
                projection.append(alias, 1);
            }
        }

        // Exclude the _id field
        projection.append("_id", 0);

        return Aggregates.project(projection);
    }

    /**
     * get the final field name for group like "fullName__last -> last"
     * @param field
     * @return simple field name without "__"
     */
    static String getSimpleName(String field) {
        if (StringUtils.isEmpty(field)) {
            return field;
        }
        return field.contains("__") ? field.substring(field.lastIndexOf("__") + 2) : field;
    }

    /**
     * If a simple count without group by, when hit is empty, convert the result from [] -> [{"count": 0}] or  [{"$1": 0}]
     *
     * <p>
     * When there is no documents to aggregate, mongodb return empty. But cosmosdb returns object indicates 0 or {}
     * </p>
     *
     * @param aggregate
     * @param results
     * @return result when hit is empty
     */
    public static List<Document> processEmptyAggregateResults(Aggregate aggregate, List<Document> results) {
        
        var ret = new Document();
        var functionParts = aggregate.function.split(",");

        for (var functionPart : functionParts) {

            var functionAndAlias = extractFunctionAndAlias(functionPart);
            var function = functionAndAlias.getLeft();
            var alias = functionAndAlias.getRight();

            var field = extractFieldFromFunction(function);
            if (StringUtils.equals(field, function)) {
                // a simple expression without aggregate
                // e.g. c['address']['state']
                // do nothing to ret
            } else if (StringUtils.startsWithIgnoreCase(function, "COUNT")) {
                // aggregate using COUNT
                // empty value for count
                ret.append(alias, 0);
            } else {
                // aggregate using other functions
                // empty value for max/min/avg
                ret.append(alias, new LinkedHashMap<String, Object>());
            }
        }

        return ret.isEmpty() ? results : List.of(ret);
    }



    /**
     * extract "SUM(c.age) AS ageSum" to function="SUM(c.age)", alias="ageSum"
     *
     * <p>
     * And also "SUM(ARRAY_LENGTH(c['children'])) AS 'count'" to function="SUM(ARRAY_LENGTH(c['children']))", alias="count"
     * </p>
     *
     * @param input
     * @return fieldName using dot
     */
    public static Pair<String, String> extractFunctionAndAlias(String input) {
        if (StringUtils.isEmpty(input)) {
            return Pair.of("", "");
        }

        var funcAndAlias = input.split(REGEX_AS);
        var function = funcAndAlias[0].trim();

        // TODO, deal with if there is no alias "SUM(c.age) without AS"
        var alias = funcAndAlias.length > 1 ? funcAndAlias[1].trim() : "";

        // removes unneeded single quotation " SUM(c.age) AS 'count' "
        alias = StringUtils.removeStart(StringUtils.removeEnd(alias, "'"), "'");

        return Pair.of(function, alias);
    }

}
