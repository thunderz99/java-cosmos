package io.github.thunderz99.cosmos.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Utility class that provides methods to create and check the existence of a table in a postgres database.
 */
public class TableUtil {
    private static final Logger log = LoggerFactory.getLogger(TableUtil.class);

    public static final String TENANT_ID = "tenant_id";
    public static final String JDOC = "jdoc";

    /**
     * Checks if a table exists in the specified schema.
     *
     * @param conn       the database connection
     * @param schemaName the schema name
     * @param tableName  the table name
     * @return true if the table exists, false otherwise
     * @throws SQLException if a database error occurs
     */
    public static boolean tableExist(Connection conn, String schemaName, String tableName) throws SQLException {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        var metaData = conn.getMetaData();
        try (var tables = metaData.getTables(null, schemaName, tableName, new String[]{"TABLE"})) {
            return tables.next(); // If there's a result, the table exists
        }
    }

    /**
     * Creates a table with the specified name and schema if it does not already exist.
     *
     * @param conn      the database connection
     * @param tableName the name of the table to create
     * @throws SQLException if a database error occurs
     */
    public static void createTableIfNotExists(Connection conn, String schemaName, String tableName) throws SQLException {

        if (!tableExist(conn, schemaName, tableName)) {

            schemaName = checkAndNormalizeValidEntityName(schemaName);
            tableName = checkAndNormalizeValidEntityName(tableName);

            // create table

            var createTableSQL = String.format("""
                CREATE TABLE %s.%s (
                    id TEXT PRIMARY KEY,
                    %s TEXT NOT NULL,
                    %s JSONB NOT NULL
                );
            """, schemaName, tableName, TENANT_ID, JDOC);

            try (PreparedStatement pstmt = conn.prepareStatement(createTableSQL)) {
                pstmt.executeUpdate();
                if (log.isInfoEnabled()) {
                    log.info("Table '{}' created successfully.", tableName);
                }
            }

            // create index on TENANT_ID for search performance
            var createIndex4TenantId = String.format("CREATE INDEX idx_tenant_id ON %s.%s (%s);", schemaName, tableName, TENANT_ID);

            try (PreparedStatement pstmt = conn.prepareStatement(createIndex4TenantId)) {
                pstmt.executeUpdate();
                if (log.isInfoEnabled()) {
                    log.info("Index on column '{}' of table '{}' created successfully.", TENANT_ID, tableName);
                }
            }

            // create jdoc index for json data search performance
            var createIndexSQL = String.format("CREATE INDEX idx_jdoc_gin ON %s.%s USING GIN (%s);", schemaName, tableName, JDOC);

            try (PreparedStatement pstmt = conn.prepareStatement(createIndexSQL)) {
                pstmt.executeUpdate();
                if (log.isInfoEnabled()) {
                    log.info("Index on column '{}' of table '{}' created successfully.", JDOC, tableName);
                }
            }
        }
    }

    /**
     * Drops a table with the specified name and schema if it exists.
     *
     * @param conn      the database connection
     * @param tableName the name of the table to drop
     * @throws SQLException if a database error occurs
     */
    public static void dropTableIfExists(Connection conn, String schemaName, String tableName) throws SQLException {

        if (tableExist(conn, schemaName, tableName)) {

            schemaName = checkAndNormalizeValidEntityName(schemaName);
            tableName = checkAndNormalizeValidEntityName(tableName);

            // drop table
            var dropTableSQL = String.format("DROP TABLE %s.%s;", schemaName, tableName);

            try (PreparedStatement pstmt = conn.prepareStatement(dropTableSQL)) {
                pstmt.executeUpdate();
                if (log.isInfoEnabled()) {
                    log.info("Table '{}' dropped successfully.", tableName);
                }
            }
        }
    }

    /**
     * Check if the given entity name is valid.And then normalize(to lower case) the name
     *
     * @param entityName the name of the entity
     * @return the normalized(to lower case) name
     */
    public static String checkAndNormalizeValidEntityName(String entityName) {

        Checker.checkNotBlank(entityName,"entityName");

        // the following characters are not allowed in entity names
        // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        Checker.check(StringUtils.containsNone(entityName, ';', ',', '&', '"', '\'', '\\'),
                "entityName should not contain invalid characters: " + entityName);

        return StringUtils.lowerCase(entityName);

    }
}
