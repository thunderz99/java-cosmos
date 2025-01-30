package io.github.thunderz99.cosmos.impl.postgres.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.dto.RecordData;
import org.apache.commons.lang3.tuple.Pair;

/**
 * context info for query, especially when join and (returnAllSubArray=false) is used.
 *
 * <p>
 *     see docs/postgres-find-with-join.md for details
 * </p>
 */
public class QueryContext extends RecordData {


    /**
     * Save the query key and params so that we can reuse these in the SELECT clause when join is used and returnAllSubArray = false.
     *
     * <p>
     * {@code
     *   {
     *     "floors.rooms":[
     *        "floor.rooms.name": {"name": "@param000_floors_rooms_name", "value": "$.floors[*].rooms[*] ? (@.name == \"r1\" || @.name == \"r2\")"},
     *        "floors.rooms.no": {"name": "@param001_floors_rooms_no", "value": "$.floors[*].rooms[*] ? (@.no > 10)"}
     *      ]
     *   }
     * }
     * </p>
     */
    public Map<String, List<Map<String, CosmosSqlParameter>>> subQueries = new LinkedHashMap<>();


    /**
     * Factory method
     * @return new FilterOptions
     */
    public static QueryContext create(){
        return new QueryContext();
    }



}
