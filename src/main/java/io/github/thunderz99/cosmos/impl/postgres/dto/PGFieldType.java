package io.github.thunderz99.cosmos.impl.postgres.dto;

/**
 * Represents PostgreSQL data types that can be used with JSONB fields.
 */
public enum PGFieldType {
    // Text types
    TEXT("text"),
    VARCHAR("varchar"),
    CHAR("char"),
    
    // Numeric types
    INTEGER("integer"),
    BIGINT("bigint"),
    NUMERIC("numeric"),
    DECIMAL("decimal"),
    REAL("real"),
    DOUBLE_PRECISION("double precision"),
    
    // Boolean
    BOOLEAN("boolean"),
    
    // Date/Time types
    TIMESTAMP("timestamp"),
    TIMESTAMPTZ("timestamptz"),
    DATE("date"),
    TIME("time"),
    TIMETZ("timetz"),
    
    // JSON types
    JSON("json"),
    JSONB("jsonb"),
    
    // Network address types
    INET("inet"),
    MACADDR("macaddr"),
    
    // UUID
    UUID("uuid");
    
    private final String typeName;
    
    PGFieldType(String typeName) {
        this.typeName = typeName;
    }
    
    @Override
    public String toString() {
        return typeName;
    }
}
