package io.github.thunderz99.cosmos.impl.postgres.dto;

/**
 * DTO class for SELECT clause. see docs/postgres-find-with-join.md for details
 */
public class SelectClause {

    /**
     * temporary table name for postgres CTE. used in the following example
     *
     * <p>
     *     {@code
     *
     *     WITH filtered_data AS (
     *       SELECT * FROM schema1.table1
     *     )
     *     SELECT id,
     *       jsonb_build_object(
     *       'id', data->'id',
     *       'address', data->'address',
     * 	     'room*no-01',data->'room*no-01'
     *     ) AS data
     *     FROM filtered_data
     *      ORDER BY data->'_ts' DESC, data->'id' DESC
     *      OFFSET 0 LIMIT 100
     *
     *     }
     * </p>
     *
     */
    public static final String FILTERED_DATA = "filtered_data";

    /**
     *  select sql text for the final select
     *
     *  <p>
     *      selectPart = "id, jsonb_build_object('address', data->'address', 'room*no-01',data->'room*no-01') AS data"
     *  </p>
     */
    public String selectPart = "*";

    /**
     *  select sql text for with clause. will be empty "" if with clause is not used.
     *
     *  <p>
     *      withClause = "WITH filtered_data AS (SELECT * FROM schema1.table1)"
     *  </p>
     */
    public String withClause = "";



    public SelectClause(){
    }

    public SelectClause(String selectPart){
        this.selectPart = selectPart;
    }


    public SelectClause(String selectPart, String withClause){
        this.selectPart = selectPart;
        this.withClause = withClause;
    }

}
