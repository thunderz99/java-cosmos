package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.impl.postgres.PostgresRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Utility class that provides methods to create and check the existence of a table in a postgres database.
 */
public class TableUtil {
    private static final Logger log = LoggerFactory.getLogger(TableUtil.class);

    /**
     * id column. pk
     */
    public static final String ID = "id";

    /**
     * tenant_id column. used to store the collectionName.
     */
    public static final String TENANT_ID = "tenant_id";

    /**
     * jdoc column. used to store the main json data
     */
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
                    %s TEXT NOT NULL,
                    %s TEXT NOT NULL,
                    %s JSONB NOT NULL,
                    PRIMARY KEY (%s, %s)
                );
            """, schemaName, tableName, ID, TENANT_ID, JDOC, TENANT_ID, ID);

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

    /**
     * Inserts a record into a table and returns the inserted record.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @param record the record to insert
     * @return the inserted record as a PostgresRecord
     * @throws SQLException if a database error occurs
     */
    public static PostgresRecord insertRecord(Connection conn, String schemaName, String tableName, PostgresRecord record) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);


        var id = record.id;
        var jdoc = JsonUtil.toJson(record.map);
        var tenantId = record.tenantId;

        // insert into table and return the result
        var insertTableSQL = String.format("""
        INSERT INTO %s.%s (%s, %s, %s)
        VALUES (?, ?, ?)
        RETURNING *;
        """, schemaName, tableName, ID, TENANT_ID, JDOC);

        try (var pstmt = conn.prepareStatement(insertTableSQL)) {
            pstmt.setString(1, id);
            pstmt.setString(2, tenantId);
            pstmt.setString(3, jdoc);

            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), resultSet.getString(TENANT_ID), JsonUtil.toMap(resultSet.getString(JDOC)));
                }
                throw new IllegalStateException("resultSet is empty when inserting record into table '%s.%s'. tenantId:%s, id:%s.".formatted(schemaName, tableName, tenantId, id));
            }
        } catch (SQLException e) {
            log.error("Error when inserting record into table '{}.{}'. tenantId:{}, id:{}.", schemaName, tableName, tenantId, id);
            throw e;
        }
    }

    /**
     * Read a record from a table by id.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @param id the id of the record
     * @return the record as a PostgresRecord
     * @throws SQLException if a database error occurs
     */
    public static PostgresRecord readRecord(Connection conn, String schemaName, String tableName, String id) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        // query the table and return the result
        var queryTableSQL = String.format("""
        SELECT * FROM %s.%s WHERE %s = ?;
        """, schemaName, tableName, ID);

        try (var pstmt = conn.prepareStatement(queryTableSQL)) {
            pstmt.setString(1, id);
            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), resultSet.getString(TENANT_ID), JsonUtil.toMap(resultSet.getString(JDOC)));
                }
                return null;
            }
        } catch (SQLException e) {
            log.error("Error when reading record from table '{}.{}'. id:{}.", schemaName, tableName, id);
            throw e;
        }
    }

    /**
     * Update a record in a table.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @param record the record to update
     * @throws SQLException if a database error occurs
     */
    public static PostgresRecord updateRecord(Connection conn, String schemaName, String tableName, PostgresRecord record) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        var id = record.id;
        var jdoc = JsonUtil.toJson(record.map);
        var tenantId = record.tenantId;

        // update table
        var updateTableSQL = String.format("""
        UPDATE %s.%s
        SET %s = ?
        WHERE %s = ? AND %s = ?
        RETURNING *;
        """, schemaName, tableName, JDOC, TENANT_ID, ID);

        try (var pstmt = conn.prepareStatement(updateTableSQL)) {
            pstmt.setString(1, jdoc);
            pstmt.setString(2, tenantId);
            pstmt.setString(3, id);

            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), resultSet.getString(TENANT_ID), JsonUtil.toMap(resultSet.getString(JDOC)));
                }
                throw new IllegalStateException("resultSet is empty(Not Found) when updating record into table '%s.%s'. tenantId:%s, id:%s.".formatted(schemaName, tableName, tenantId, id));
            }
        } catch (SQLException e) {
            log.error("Error when updating record in table '{}.{}'. tenantId:{}, id:{}.", schemaName, tableName, tenantId, id);
            throw e;
        }
    }


    /**
     * Delete a record in a table.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @param id the id of the record to delete
     * @throws SQLException if a database error occurs
     */
    public static String deleteRecord(Connection conn, String schemaName, String tableName, String tenantId, String id) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        // delete record
        var deleteRecordSQL = String.format("""
        DELETE FROM %s.%s
        WHERE %s = ? AND %s = ?;
        """, schemaName, tableName, TENANT_ID, ID);

        try (var pstmt = conn.prepareStatement(deleteRecordSQL)) {
            pstmt.setString(1, tenantId);
            pstmt.setString(2, id);
            var count = pstmt.executeUpdate();
            if (count == 0 && log.isInfoEnabled()) {
                log.info("record not found when deleting from table '{}.{}}'. tenantId:{}, id:{}}.".formatted(schemaName, tableName, tenantId, id));
            }
            return id;
        } catch (SQLException e) {
            log.error("Error when deleting record from table '{}.{}'. tenantId:{}, id:{}.", schemaName, tableName, tenantId, id);
            throw e;
        }
    }

}
