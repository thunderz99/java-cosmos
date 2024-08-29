package io.github.thunderz99.cosmos.util;

import java.util.ArrayList;
import java.util.HashSet;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;
import io.github.thunderz99.cosmos.condition.Aggregate;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * A util class that generate bson data for aggregate pipeline
 */
public class AggregateUtil {

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
            var fieldInPipeline = convertFieldNameIncludingDot(groupByField);

            if(!projectionSet.contains(fieldInPipeline)) {
                // add the alias and field only if not added already
                projection.append(fieldInPipeline, "$" + groupByField);
                projectionSet.add(fieldInPipeline);
            }
        }

        // Include all fields that will be used in aggregate functions
        var functionParts = aggregate.function.split(",");
        for (var functionPart : functionParts) {
            var function = functionPart.trim().split("\\s+AS\\s+")[0];
            var field = function.substring(function.indexOf('(') + 1, function.indexOf(')')).trim();
            // Remove the heading "c." which is only used in cosmosdb
            field = StringUtils.removeStart(field, "c.");

            var fieldInPipeline = convertFieldNameIncludingDot(field);
            if(!projectionSet.contains(fieldInPipeline)) {
                // add the alias and field only if not added already
                projection.append(fieldInPipeline, "$" + field);
                projectionSet.add(fieldInPipeline);
            }
        }

        return Aggregates.project(projection);
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
     * @param aggregate
     * @return group stage in bson
     */
    public static Bson createGroupStage(Aggregate aggregate) {
        if (aggregate.groupBy.isEmpty() || aggregate.function.isEmpty()) {
            return null;
        }

        var functionParts = aggregate.function.split(",");
        var accumulators = new ArrayList<BsonField>();

        // Add accumulators for each aggregate function
        for (var functionPart : functionParts) {
            var funcAndAlias = functionPart.split("\\s+AS\\s+");
            var function = funcAndAlias[0].trim();
            var alias = funcAndAlias.length > 1 ? funcAndAlias[1].trim() : function;

            if (function.startsWith("COUNT")) {
                accumulators.add(Accumulators.sum(alias, 1));
            } else {
                var field = function.substring(function.indexOf('(') + 1, function.indexOf(')')).trim();

                // Remove the c. prefix used in cosmosdb
                field = StringUtils.removeStart(field, "c.");

                var fieldInPipeline = convertFieldNameIncludingDot(field);

                if (function.startsWith("MAX")) {
                    accumulators.add(Accumulators.max(alias, "$" + fieldInPipeline));
                } else if (function.startsWith("MIN")) {
                    accumulators.add(Accumulators.min(alias, "$" + fieldInPipeline));
                } else if (function.startsWith("SUM")) {
                    accumulators.add(Accumulators.sum(alias, "$" + fieldInPipeline));
                } else if (function.startsWith("AVG")) {
                    accumulators.add(Accumulators.avg(alias, "$" + fieldInPipeline));
                }
            }
        }

        // Create the group key based on the groupBy fields (use renamed fields where necessary)
        var groupId = new Document();
        for (var groupByField : aggregate.groupBy) {
            var fieldInPipeline = convertFieldNameIncludingDot(groupByField);
            groupId.append(fieldInPipeline, "$" + fieldInPipeline);
        }

        // Create the group stage
        return Aggregates.group(groupId, accumulators);
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
            var fieldInPipeline = convertFieldNameIncludingDot(groupByField);
            var finalFieldName = getSimpleName(fieldInPipeline);
            projection.append(finalFieldName, "$_id." + fieldInPipeline);
        }

        // Include all calculated fields (e.g., facetCount, maxAge) directly from the group stage
        var functionParts = aggregate.function.split(",");
        var index = 1;
        for (var functionPart : functionParts) {
            var funcAndAlias = functionPart.split("\\s+AS\\s+");
            var function = funcAndAlias[0];
            if(funcAndAlias.length >1){
                // `max(c.age) AS maxAge` will be maxAge
                var alias = funcAndAlias[1].trim();
                projection.append(alias, 1);
            } else {
                // `max(c.age)` without AS will be $1 or $2, etc. According to cosmosdb's spec
                // TODO $1 and $2 not implemented
                var alias = function;
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
        if(StringUtils.isEmpty(field)){
            return field;
        }
        return field.contains("__") ? field.substring(field.lastIndexOf("__") + 2) : field;
    }
}
