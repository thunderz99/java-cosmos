package io.github.thunderz99.cosmos.impl.postgres.dto;

/**
 * Represents a PostgreSQL index field configuration, including its name (JSON path) and data type for casting.
 */
public class PGIndexField {
    /**
     * The name of the field, which can be a nested JSON path (e.g., "user.address.city").
     */
    public final String fieldName;
    
    /**
     * The PostgreSQL data type to cast the field to in the index definition.
     */
    public final PGFieldType fieldType;

    /**
     * Creates a new PGIndexField with the specified field name and type.
     *
     * @param fieldName the name of the field (JSON path)
     * @param fieldType the PostgreSQL data type for the field
     * @return a new PGIndexField instance
     * @throws NullPointerException if either fieldName or fieldType is null
     */
    public static PGIndexField of(String fieldName, PGFieldType fieldType) {
        return new PGIndexField(fieldName, fieldType);
    }

    /**
     * Constructs an IndexField with a specified field name and type.
     *
     * @param fieldName The name of the field (JSON path).
     * @param fieldType The PostgreSQL data type for casting.
     */
    public PGIndexField(String fieldName, PGFieldType fieldType) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
    }

    /**
     * Constructs an IndexField with a specified field name and a default type of TEXT.
     *
     * @param fieldName The name of the field (JSON path).
     */
    public PGIndexField(String fieldName) {
        this(fieldName, PGFieldType.TEXT); // Default to TEXT
    }
}
