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
     * tenantId of record. collection id will be set, representing a tenant for SaaS system.
     *
     * <p>
     *     if single tenant, "" should be set
     * </p>
     */
    public String tenantId;

    /**
     * map representing the main json data, e.g. {"address": {"city": "New York"}}
     */
    public Map<String, Object> data;

    public PostgresRecord(){

    }

    public PostgresRecord(String id, String tenantId, Map<String, Object> data) {
        this.id = id;
        this.tenantId = tenantId;
        this.data = data;
    }
}
