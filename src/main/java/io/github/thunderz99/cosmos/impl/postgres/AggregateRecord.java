package io.github.thunderz99.cosmos.impl.postgres;

import io.github.thunderz99.cosmos.impl.postgres.util.TableUtil;
import io.github.thunderz99.cosmos.util.Checker;

import java.util.Map;

/**
 * An aggregate result record for postgres
 */
public class AggregateRecord {

    /**
     * pk of record. usually uuid.
     */
    public String id;

    /**
     * map representing the main json data, e.g. {"address": {"city": "New York"}}
     */
    public Map<String, Object> data;

    public AggregateRecord(){

    }

    public AggregateRecord(String id, Map<String, Object> data) {
        // row id
        this.id = id;
        Checker.checkNotNull(data, "data");
        // the aggregate data
        // unlike PostgresRecord, the "id" field will not be put back to the data
        // because row id is different from the "id" in aggregate data
        this.data = data;
    }
}
