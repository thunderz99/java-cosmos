package io.github.thunderz99.cosmos.impl.postgres.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.thunderz99.cosmos.condition.BucketAggregateFunction;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregate;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregateValidator;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import org.apache.commons.lang3.StringUtils;

/**
 * Builds PostgreSQL FILTER aggregate queries for multiple independent buckets.
 */
public final class PGMultiBucketUtil {

    private PGMultiBucketUtil() {
    }

    public static CosmosSqlQuerySpec toQuerySpec(String coll, MultiBucketAggregate aggregate,
                                                 Condition sharedCondition, String partition) {
        MultiBucketAggregateValidator.validate(aggregate, sharedCondition);

        var schema = TableUtil.checkAndNormalizeValidEntityName(coll);
        var table = TableUtil.checkAndNormalizeValidEntityName(partition);
        var params = new ArrayList<CosmosSqlParameter>();
        var paramIndex = new AtomicInteger(0);
        var queryContext = QueryContext.create();
        queryContext.schemaName = schema;
        queryContext.tableName = table;

        var sharedPredicate = compileBarePredicate(sharedCondition, params, paramIndex, queryContext);
        String formattedField = null;
        String numericFieldPredicate = null;
        if (aggregate.function != BucketAggregateFunction.COUNT) {
            var normalizedField = MultiBucketAggregateValidator.normalizeField(aggregate.field);
            formattedField = PGKeyUtil.getFormattedKeyWithAlias(normalizedField, TableUtil.DATA, 0);
            numericFieldPredicate = "jsonb_typeof(%s) = 'number'"
                    .formatted(PGKeyUtil.getFormattedKey4JsonWithAlias(normalizedField, TableUtil.DATA));
        }

        var selectColumns = new ArrayList<String>();
        for (var i = 0; i < aggregate.buckets.size(); i++) {
            var predicate = compileBarePredicate(aggregate.buckets.get(i).condition, params, paramIndex, queryContext);
            if (StringUtils.isBlank(predicate)) {
                predicate = "TRUE";
            }

            var alias = "b" + i;
            selectColumns.add("COUNT(*) FILTER (WHERE %s) AS \"%s_matched\"".formatted(predicate, alias));
            if (aggregate.function != BucketAggregateFunction.COUNT) {
                selectColumns.add("%s(%s) FILTER (WHERE (%s) AND %s) AS \"%s\""
                        .formatted(aggregate.function, formattedField, predicate, numericFieldPredicate, alias));
            }
        }

        var queryText = new StringBuilder("SELECT ")
                .append(String.join(",\n", selectColumns))
                .append("\nFROM ").append(schema).append(".").append(table);
        if (StringUtils.isNotBlank(sharedPredicate)) {
            queryText.append("\nWHERE ").append(sharedPredicate);
        }

        MultiBucketAggregateValidator.checkCompiledRequest(queryText.toString(), params);
        return new CosmosSqlQuerySpec(queryText.toString(), params);
    }

    private static String compileBarePredicate(Condition condition, List<CosmosSqlParameter> params,
                                               AtomicInteger paramIndex, QueryContext queryContext) {
        var conditionIndex = new AtomicInteger(0);
        var filterQuery = PGConditionUtil.generateFilterQuery(condition, "", params, conditionIndex,
                paramIndex, queryContext);
        return PGConditionUtil.removeConnectPart(filterQuery.queryText.toString());
    }
}
