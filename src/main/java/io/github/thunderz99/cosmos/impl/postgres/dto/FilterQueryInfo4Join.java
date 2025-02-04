package io.github.thunderz99.cosmos.impl.postgres.dto;

import io.github.thunderz99.cosmos.condition.Expression;
import io.github.thunderz99.cosmos.condition.Filter;
import io.github.thunderz99.cosmos.condition.FilterQuery;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * When join and (returnAllSubArray=false) is used, this class is used to store the query information.
 *
 * <p>
 * Because we need to filter out the subArray when join and (returnAllSubArray=false),
 * and the filter condition is the SELECT clause is almost the same as the WHERE clause.
 * We save this information in this class, in order to let the SELECT clause generation be easier.
 *
 * see docs/postgres-find-with-join.md for details
 * </p>
 */
public class FilterQueryInfo4Join {


    public FilterQueryInfo4Join(){
    }

    public FilterQueryInfo4Join(String baseKey, String remainedJoinKey, String filterKey, AtomicInteger paramIndex, Expression subExp){
        this.baseKey = baseKey;
        this.remainedJoinKey = remainedJoinKey;
        this.filterKey = filterKey;
        this.paramIndex = paramIndex;
        this.subExp = subExp;
    }

    /**
     * baseKey for join. e.g. "floors"
     */
    public String baseKey;


    /**
     * remainedKey for join. e.g. "rooms"  ("floors.rooms" whose baseKey part is removed)
     */
    public String remainedJoinKey;

    /**
     * filterKey for ARRAY_CONTAINS_ANY or ARRAY_CONTAINS_ALL
     * could be empty.
     * e.g. "name".
     *
     * <p>
     * {@code
     *   // in this case
     *   // baseKey = "floors"
     *   // remainedJoinKey = "rooms"
     *   // filterKey = "name"
     *   Condition.filter"floors.rooms ARRAY_CONTAINS_ANY name", List.of("r1", "r2")).join(Set.of("floors"))
     * }
     * </p>
     */
    public String filterKey;

    /**
     * the numbering for param000, param001, param002 and so on.
     */
    public AtomicInteger paramIndex;

    /**
     * The expression for WHERE clause. (PGSimpleExpression or PGSubQueryExpression)
     *
     * <p>
     *     save this expression in order to let the SELECT clause generation be easier.
     *     because the subExp used in SELECT clause is almost the same as the subExp in WHERE clause.
     * </p>
     *
     * <p>
     *     example for PGSimpleExpression: Condition.filter("name", "001", "age >" 20)
     *     example for PGSubQueryExpression: Condition.filter("rooms ARRAY_CONTAINS_ANY name", List.of("r1", "r2"))
     *
     * </p>
     */
    public Expression subExp;
}
