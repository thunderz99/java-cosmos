package io.github.thunderz99.cosmos.impl.postgres;

import java.util.Map;

/**
 * A standard record for postgres table in java-cosmos
 */
public class PostgresRecord {

    /**
     * pk of record. usually uuid.
     */
    public String id;

    /**
     * map representing the main json data, e.g. {"address": {"city": "New York"}}
     */
    public Map<String, Object> data;

    public PostgresRecord(){

    }

    public PostgresRecord(String id, Map<String, Object> data) {
        this.id = id;
        this.data = data;
    }
}
