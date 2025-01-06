package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.impl.postgres.PostgresRecord;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
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
     * data column(JSONB type). used to store the main json data
     */
    public static final String DATA = "data";

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
                    %s TEXT NOT NULL PRIMARY KEY,
                    %s JSONB NOT NULL
                )
            """, schemaName, tableName, ID, DATA);

            try (PreparedStatement pstmt = conn.prepareStatement(createTableSQL)) {
                pstmt.executeUpdate();
                if (log.isInfoEnabled()) {
                    log.info("Table '{}' created successfully.", tableName);
                }
            }

            {
                // create data index for json data search performance
                var indexName = String.format("idx_%s_data", tableName);
                var createIndexSQL = String.format("CREATE INDEX %s ON %s.%s USING GIN (%s);", indexName, schemaName, tableName, DATA);

                try (PreparedStatement pstmt = conn.prepareStatement(createIndexSQL)) {
                    pstmt.executeUpdate();
                    if (log.isInfoEnabled()) {
                        log.info("Index({}) on column '{}' of table '{}' created successfully.", indexName, DATA, tableName);
                    }
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

        checkValidRecord(record);

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        var id = record.id;
        var map = new HashMap<>(record.data);

        // add ID fields to map, for the compatibility with CosmosDB/MongoDB
        map.put(ID, id);
        var json = JsonUtil.toJson(map);

        // insert into table and return the result
        var insertTableSQL = String.format("""
        INSERT INTO %s.%s (%s, %s)
        VALUES (?, ?)
        RETURNING *
        """, schemaName, tableName, ID, DATA);

        try (var pstmt = conn.prepareStatement(insertTableSQL)) {
            pstmt.setString(1, id);
            pstmt.setObject(2, json, Types.OTHER);

            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                }
                throw new IllegalStateException("resultSet is empty when inserting record into table '%s.%s'. id:%s.".formatted(schemaName, tableName, id));
            }
        } catch (SQLException e) {
            log.warn("Error when inserting record into table '{}.{}'. id:{}.", schemaName, tableName, id, e);
            throw e;
        }
    }

    static void checkValidRecord(PostgresRecord record) {
        Checker.checkNotNull(record, "record");
        Checker.checkNotEmpty(record.id, "record.id");
        Checker.checkNotNull(record.data, "record.map");
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
        return readRecord(conn, schemaName, tableName, id, false);
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
    public static PostgresRecord readRecord(Connection conn, String schemaName, String tableName, String id, boolean selectForUpdate) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotEmpty(id, "id");

        // query the table and return the result
        var queryTableSQL = String.format("""
        SELECT * FROM %s.%s WHERE %s = ?
        """, schemaName, tableName, ID);

        if(selectForUpdate){
            queryTableSQL = queryTableSQL + " FOR UPDATE";
        }

        try (var pstmt = conn.prepareStatement(queryTableSQL)) {
            pstmt.setString(1, id);
            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                }
                return null;
            }
        } catch (SQLException e) {
            log.warn("Error when reading record from table '{}.{}'. id:{}.", schemaName, tableName, id, e);
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

        checkValidRecord(record);

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        var id = record.id;
        var map = new HashMap<>(record.data);

        // add ID fields to map, for the compatibility with CosmosDB/MongoDB
        map.put(ID, id);
        var data = JsonUtil.toJson(map);

        // update table
        var updateTableSQL = String.format("""
        UPDATE %s.%s
        SET %s = ?
        WHERE %s = ?
        RETURNING *
        """, schemaName, tableName, DATA, ID);

        try (var pstmt = conn.prepareStatement(updateTableSQL)) {
            pstmt.setObject(1, data, Types.OTHER);
            pstmt.setString(2, id);

            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                }
                throw new IllegalStateException("resultSet is empty(Not Found) when updating record into table '%s.%s'. id:%s.".formatted(schemaName, tableName, id));
            }
        } catch (SQLException e) {
            log.warn("Error when updating record in table '{}.{}'. id:{}.", schemaName, tableName, id, e);
            throw e;
        }
    }


    /**
     * Partially update a record's data column in a table.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @param record partial data to update
     * @throws Exception if a database error occurs
     */
    public static PostgresRecord updatePartialRecord(Connection conn, String schemaName, String tableName, PostgresRecord record) throws Exception {
        checkValidRecord(record);

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        try {

            conn.setAutoCommit(false);
            var existRecord = readRecord(conn, schemaName, tableName, record.id, true);

            if (existRecord == null) {
                throw new CosmosException(404, "404", "record 404 Not Found when updating partial record in table '%s.%s'. id:%s.".formatted(schemaName, tableName, record.id));
            }

            var id = record.id;
            var newMap = new HashMap<>(record.data);
            // add ID fields to map, for the compatibility with CosmosDB/MongoDB
            newMap.put(ID, id);

            // like Object.assign(m1, m2) in javascript, but support nested merge.
            var merged = MapUtil.merge(existRecord.data, newMap);

            var data = JsonUtil.toJson(merged);

            var updateTableSQL = String.format("""
            UPDATE %s.%s
            SET %s = ?
            WHERE %s = ?
            RETURNING *
            """, schemaName, tableName, DATA, ID);

            try (var pstmt = conn.prepareStatement(updateTableSQL)) {
                pstmt.setObject(1, data, Types.OTHER);
                pstmt.setString(2, id);

                // Execute the query and return the result
                try(var resultSet = pstmt.executeQuery()) {
                    if (resultSet.next()) {
                        var newRecord = new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                        conn.commit();
                        return newRecord;
                    }
                    throw new IllegalStateException("resultSet is empty(Not Found) when updating partial record in table '%s.%s'. id:%s.".formatted(schemaName, tableName, id));
                }
            }
        } catch (Exception e) {
            log.warn("Error when updating partial record in table '{}.{}'. id:{}.", schemaName, tableName, record.id, e);
            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                log.error("Failed to rollback in table '{}.{}'. id:{}.", schemaName, tableName, record.id, rollbackEx);
            }
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * Upsert. Insert a record into a table if not exist, otherwise update the record.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @param record the record to upsert
     * @throws SQLException if a database error occurs
     */
    public static PostgresRecord upsertRecord(Connection conn, String schemaName, String tableName, PostgresRecord record) throws Exception {

        checkValidRecord(record);

        var id = record.id;
        var map = new HashMap<>(record.data);

        // add ID fields to map, for the compatibility with CosmosDB/MongoDB
        map.put(ID, id);
        var data = JsonUtil.toJson(map);

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        // insert into table and return the result
        var insertTableSQL = String.format("""
        INSERT INTO %s.%s (%s,%s)
        VALUES (?, ?)
        ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s
        RETURNING *
        """, schemaName, tableName, ID, DATA, ID, DATA, DATA);

        try (var pstmt = conn.prepareStatement(insertTableSQL)) {
            pstmt.setString(1, id);
            pstmt.setObject(2, data, Types.OTHER);

            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                }
                throw new IllegalStateException("resultSet is empty when upserting record into table '%s.%s'. id:%s.".formatted(schemaName, tableName, id));
            }
        } catch (SQLException e) {
            log.warn("Error when upserting record in table '{}.{}'. id:{}.", schemaName, tableName, id, e);
            throw e;
        }
    }

    /**
     * patch a record using JsonPatch format.
     *
     * @param conn the database connection
     * @param schemaName the name of the schema
     * @param tableName the name of the table
     * @throws Exception if a database error occurs
     */
    public static PostgresRecord patchRecord(Connection conn, String schemaName, String tableName, String id, PatchOperations operations) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotEmpty(id, "id");
        Checker.checkNotNull(operations, "operations");

        if(CollectionUtils.isEmpty(operations.getPatchOperations())){
            log.warn("operations is empty. do nothing in table '{}.{}'. id:{}.", schemaName, tableName, id);
            return new PostgresRecord(id, Map.of());
        }

        var querySpec = JsonPatchUtil.toPostgresPatchData(operations);

        // from named to positional
        // for example:
        // "SELECT * FROM table WHERE id = @id AND name = @name"
        // to
        // "SELECT * FROM table WHERE id = ? AND name = ?"
        querySpec = NamedParameterUtil.convert(querySpec);

        var subSql = querySpec.queryText;
        var params = querySpec.params;

        // update table's data column and return the result
        var patchTableSQL = String.format("""
        UPDATE %s.%s 
        SET %s = %s
        WHERE %s = ?
        RETURNING *
        """, schemaName, tableName, DATA, subSql, ID);

        try (var pstmt = conn.prepareStatement(patchTableSQL)) {

            var index = 1;
            for(var param : params){
                if(param.value instanceof String strValue){
                    pstmt.setString(index, "\"" + strValue + "\"");
                } else {
                    pstmt.setString(index, JsonUtil.toJson(param.value));
                }
                index ++;
            }

            // add params used in WHERE clause
            pstmt.setString(index, id);

            // Execute the query and return the result
            try(var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                }
                throw new CosmosException(404, "404", "resultSet is 404 Not Found when patch record into table '%s.%s'. id:%s.".formatted(schemaName, tableName, id));
            }
        } catch (SQLException e) {
            log.warn("Error when patch record in table '{}.{}'. id:{}.", schemaName, tableName, id, e);
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
    public static String deleteRecord(Connection conn, String schemaName, String tableName, String id) throws Exception {

        Checker.checkNotEmpty(id, "id");

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        // delete record
        var deleteRecordSQL = String.format("""
        DELETE FROM %s.%s
        WHERE %s = ?
        """, schemaName, tableName, ID);

        try (var pstmt = conn.prepareStatement(deleteRecordSQL)) {
            pstmt.setString(1, id);
            var count = pstmt.executeUpdate();
            if (count == 0 && log.isInfoEnabled()) {
                log.info("record not found when deleting from table '{}.{}}'. id:{}}.".formatted(schemaName, tableName, id));
            }
            return id;
        } catch (SQLException e) {
            log.warn("Error when deleting record from table '{}.{}'. id:{}.", schemaName, tableName, id);
            throw e;
        }
    }

}
