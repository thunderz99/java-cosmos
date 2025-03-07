package io.github.thunderz99.cosmos.impl.postgres.dto;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.thunderz99.cosmos.dto.RecordData;
import io.github.thunderz99.cosmos.impl.postgres.PostgresDatabaseImpl;

/**
 * context info for query, especially when join and (returnAllSubArray=false) is used.
 *
 * <p>
 *     see docs/postgres-find-with-join.md for details
 * </p>
 */
public class QueryContext extends RecordData {

    /**
     * {@code
     * {
     *   "floors":[
     *      {
     *        "baseKey": "floors",
     *        "remainedJoinKey": "rooms",
     *        "filterKey": "name",
     *        "paramIndex": paramIndex,
     *        "subExp": PGSimpleExpression | PGSubQueryExpression
     *      },
     *      {
     *        "baseKey": "floors",
     *        "remainedJoinKey": "rooms",
     *        "filterKey": "name",
     *        "paramIndex": paramIndex,
     *        "subExp": PGSimpleExpression | PGSubQueryExpression
     *      }
     *    ],
     *    "children":[
     *      {
     *        "baseKey": "children",
     *        "remainedJoinKey": "",
     *        "filterKey": "age",
     *        "paramIndex": paramIndex,
     *        "subExp": PGSimpleExpression | PGSubQueryExpression
     *      }
     *    ]
     *
     * }
     * }
     */
    public Map<String, List<FilterQueryInfo4Join>> filterQueryInfos4Join = new LinkedHashMap<>();

    /**
     * Whether the query is in "after aggregation" context
     *
     * <p>
     *     if in after aggregation context, the filter key should differ from the original data->xxx format
     *     {@code
     *         // if false: WHERE (data->>'facetCount')::int > @param000_facetCount
     *         // if true: WHERE "facetCount" > @param000_facetCount
     *     }
     *
     * </p>
     */
    public boolean afterAggregation = false;

    public String schemaName;

    public String tableName;

    /**
     * databaseImpl to be set in the context
     *
     * <p>
     *     when doing aggregate query, we need to use the databaseImpl to determine the type of the min/max target field
     * </p>
     * <p>
     *     this field is not a simple data dto. so it should not be serialized
     * </p>
     */
    @JsonIgnore
    public PostgresDatabaseImpl databaseImpl;

    /**
     * Factory method
     * @return new FilterOptions
     */
    public static QueryContext create(){
        return new QueryContext();
    }


    /**
     * set the afterAggregation context
     * @param afterAggregation
     * @return self
     */
    public QueryContext afterAggregation(boolean afterAggregation){
        this.afterAggregation = afterAggregation;
        return this;
    }

    /**
     * set the databaseImpl context
     * @param databaseImpl
     * @return self
     */
    public QueryContext databaseImpl(PostgresDatabaseImpl databaseImpl){
        this.databaseImpl = databaseImpl;
        return this;
    }



}
