package io.github.thunderz99.cosmos.impl.postgres.util;

import com.google.common.collect.Maps;
import io.github.thunderz99.cosmos.CosmosDocument;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosBulkResult;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.impl.postgres.PostgresRecord;
import io.github.thunderz99.cosmos.impl.postgres.dto.IndexOption;
import io.github.thunderz99.cosmos.util.*;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

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
     * field automatically added to contain the etag value for optimistic lock
     */
    public static final String ETAG = "_etag";

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

        var schemaNameWithoutQuotes = removeQuotes(schemaName);
        var tableNameWithoutQuotes = removeQuotes(tableName);

        var metaData = conn.getMetaData();
        try (var tables = metaData.getTables(null, schemaNameWithoutQuotes, tableNameWithoutQuotes, new String[]{"TABLE"})) {
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
    public static String createTableIfNotExists(Connection conn, String schemaName, String tableName) throws SQLException {

        if (tableExist(conn, schemaName, tableName)) {
            // already exists
            return "";
        }

        // create table

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
                log.info("Table '{}.{}' created successfully.", schemaName, tableName);
            }
        }

        {
            // create data index for json data search performance
            var indexName = getIndexName(tableName, DATA);
            var createIndexSQL = String.format("CREATE INDEX %s ON %s.%s USING GIN (%s);", indexName, schemaName, tableName, DATA);

            try (PreparedStatement pstmt = conn.prepareStatement(createIndexSQL)) {
                pstmt.executeUpdate();
                if (log.isInfoEnabled()) {
                    log.info("Index({}) on column '{}' of table '{}.{}' created successfully.", indexName, DATA, schemaName, tableName);
                }
            }
        }

        return "%s.%s".formatted(schemaName, tableName);
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
                    log.info("Table '{}.{}' dropped successfully.", schemaName, tableName);
                }
            }
        }
    }

    /**
     * Check if the given entity name is valid. And then normalize(to lower case) the name. if entityName's length is over 63, it will be shortened using a hash method.
     *
     * @param entityName the name of the entity
     * @return the normalized(to lower case) name
     */
    public static String checkAndNormalizeValidEntityName(String entityName) {
        checkValidEntityName(entityName);

        entityName = removeQuotes(entityName);

        // get a shortened version of entityName is length > 63 chars
        entityName = getShortenedEntityName(entityName);

        if(isSimpleEntityName(entityName)) {
            // if all lower case and no "-", no need to use double quotation
            return entityName;
        }

        // use double quotation to let the entityName be a valid postgres table name. which supports upper character and "-"
        return "\"%s\"".formatted(entityName);

    }

    /**
     * Check if the given entity name is valid.(only check)
     *
     * @param entityName the name of the entity
     */

    static void checkValidEntityName(String entityName) {
        Checker.checkNotBlank(entityName, "entityName");

        // the following characters are not allowed in entity names
        // https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        Checker.check(StringUtils.containsNone(entityName, ';', ',', '&', '\'', '\\', '(', ')', '\t', '\n', '\r'),
                "entityName should not contain invalid characters: " + entityName);

        Checker.check(!StringUtils.contains(entityName, "--"),
                "entityName should not contain '--': " + entityName);
    }

    /**
     * if entityName's length > 63 (postgres table/index name limit), return a shortened version of entityName by hashing a part of the entityName.
     *
     * @param entityName
     * @return a shortened entityName
     */
    static String getShortenedEntityName(String entityName) {

        var length = StringUtils.length(entityName);

        if(length < 64){
            return entityName;
        }

        var prefix = StringUtils.substring(entityName, 0, 32);
        var suffix = StringUtils.substring(entityName, length - 8, length);
        var hash = HashUtil.toShortHash(entityName);

        return "%s_%s_%s".formatted(prefix, hash, suffix);
    }

    /**
     * Inserts a record into a table and returns the inserted record.
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param record     the record to insert
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
        var insertSQL = String.format("""
                INSERT INTO %s.%s (%s, %s)
                VALUES (?, ?)
                RETURNING *
                """, schemaName, tableName, ID, DATA);

        try (var pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setString(1, id);
            pstmt.setObject(2, json, Types.OTHER);

            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
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
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param id         the id of the record
     * @return the record as a PostgresRecord
     * @throws SQLException if a database error occurs
     */
    public static PostgresRecord readRecord(Connection conn, String schemaName, String tableName, String id) throws Exception {
        return readRecord(conn, schemaName, tableName, id, false);
    }

    /**
     * Read a record from a table by id.
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param id         the id of the record
     * @return the record as a PostgresRecord
     * @throws SQLException if a database error occurs
     */
    public static PostgresRecord readRecord(Connection conn, String schemaName, String tableName, String id, boolean selectForUpdate) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotEmpty(id, "id");

        // query the table and return the result
        var querySQL = String.format("""
                SELECT * FROM %s.%s WHERE %s = ?
                """, schemaName, tableName, ID);

        if (selectForUpdate) {
            querySQL = querySQL + " FOR UPDATE";
        }

        try (var pstmt = conn.prepareStatement(querySQL)) {
            pstmt.setString(1, id);
            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
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
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param record     the record to update
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
        var updateSQL = String.format("""
                UPDATE %s.%s
                SET %s = ?
                WHERE %s = ?
                RETURNING *
                """, schemaName, tableName, DATA, ID);

        try (var pstmt = conn.prepareStatement(updateSQL)) {
            pstmt.setObject(1, data, Types.OTHER);
            pstmt.setString(2, id);

            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
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
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param record     partial data to update
     * @throws Exception if a database error occurs
     */
    public static PostgresRecord updatePartialRecord(Connection conn, String schemaName, String tableName, PostgresRecord record) throws Exception {
        return updatePartialRecord(conn, schemaName, tableName, record, new PartialUpdateOption(), "");
    }

    /**
     * Partially update a record's data column in a table.
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param record     partial data to update
     * @param option     partial update option
     * @param etag       etag for optimistic concurrency check
     * @throws Exception if a database error occurs
     */
    public static PostgresRecord updatePartialRecord(Connection conn, String schemaName, String tableName, PostgresRecord record, PartialUpdateOption option, String etag) throws Exception {
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

            String updateSQL;
            if(option.checkETag && StringUtils.isNotEmpty(etag)) {
                // check etag before update
                updateSQL = String.format("""
                        UPDATE %s.%s
                        SET %s = ?
                        WHERE %s = ? AND %s ->> '%s' = ?
                        RETURNING *
                        """, schemaName, tableName, DATA, ID, DATA, ETAG);
            } else {
                updateSQL = String.format("""
                        UPDATE %s.%s
                        SET %s = ?
                        WHERE %s = ?
                        RETURNING *
                        """, schemaName, tableName, DATA, ID);
            }


            try (var pstmt = conn.prepareStatement(updateSQL)) {
                pstmt.setObject(1, data, Types.OTHER);
                pstmt.setString(2, id);

                if(option.checkETag && StringUtils.isNotEmpty(etag)){
                    pstmt.setString(3, etag);
                }

                // Execute the query and return the result
                try (var resultSet = pstmt.executeQuery()) {
                    if (resultSet.next()) {
                        var newRecord = new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA)));
                        conn.commit();
                        return newRecord;
                    }
                    if(option.checkETag) {
                        // etag not match
                        throw new CosmosException(412, "412 Precondition Failed", "failed to updatePartial because etag not match. table:'%s.%s', id:%s, etag:%s".formatted(schemaName, tableName, id, etag));
                    } else {
                        throw new IllegalStateException("resultSet is empty(Not Found) when updating partial record in table '%s.%s'. id:%s.".formatted(schemaName, tableName, id));
                    }
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
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param record     the record to upsert
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

        // upsert into table and return the result
        var upsertSQL = String.format("""
                INSERT INTO %s.%s (%s,%s)
                VALUES (?, ?)
                ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s
                RETURNING *
                """, schemaName, tableName, ID, DATA, ID, DATA, DATA);

        try (var pstmt = conn.prepareStatement(upsertSQL)) {
            pstmt.setString(1, id);
            pstmt.setObject(2, data, Types.OTHER);

            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
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
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @throws Exception if a database error occurs
     */
    public static PostgresRecord patchRecord(Connection conn, String schemaName, String tableName, String id, PatchOperations operations) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotEmpty(id, "id");
        Checker.checkNotNull(operations, "operations");

        if (CollectionUtils.isEmpty(operations.getPatchOperations())) {
            log.warn("operations is empty. do nothing in table '{}.{}'. id:{}.", schemaName, tableName, id);
            return new PostgresRecord(id, new HashMap<>());
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
            for (var param : params) {
                if (param.value instanceof String strValue) {
                    pstmt.setString(index, "\"" + strValue + "\"");
                } else {
                    pstmt.setString(index, JsonUtil.toJson(param.value));
                }
                index++;
            }

            // add params used in WHERE clause
            pstmt.setString(index, id);

            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
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
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param id         the id of the record to delete
     * @throws SQLException if a database error occurs
     */
    public static String deleteRecord(Connection conn, String schemaName, String tableName, String id) throws Exception {

        Checker.checkNotEmpty(id, "id");

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        // delete record
        var deleteSQL = String.format("""
                DELETE FROM %s.%s
                WHERE %s = ?
                """, schemaName, tableName, ID);

        try (var pstmt = conn.prepareStatement(deleteSQL)) {
            pstmt.setString(1, id);
            var count = pstmt.executeUpdate();
            if (count == 0 && log.isInfoEnabled()) {
                log.info("record not found when deleting from table '{}.{}}'. id:{}}.", schemaName, tableName, id);
            }
            return id;
        } catch (SQLException e) {
            log.warn("Error when deleting record from table '{}.{}'. id:{}.", schemaName, tableName, id);
            throw e;
        }
    }

    /**
     * Insert a batch of records into a table(Transaction is managed in this method).
     *
     * <p>
     * This method will call commit automatically. If you want to manage transaction by yourself, please use bulkInsertRecords
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param records    the list of records to insert
     * @return an array of CosmosDocument representing the number of rows inserted for each record
     * @throws Exception if a database error occurs
     */
    public static List<CosmosDocument> batchInsertRecords(Connection conn, String schemaName, String tableName, List<PostgresRecord> records) throws Exception {

        try {
            conn.setAutoCommit(false); // start transaction
            var ret = _insertRecords(conn, schemaName, tableName, records, true);

            conn.commit(); // do the commit
            return ret.successList;

        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback(); // do rollback if failed
                    log.warn("Transaction rolled back due to an error when batch inserting records into table '{}.{}'. records size:{}.", schemaName, tableName, records.size());
                }
            } catch (SQLException rollbackEx) {
                log.error("Failed to roll back transaction when batch inserting records into table '{}.{}'. records size:{}.", schemaName, tableName, records.size(), e);
            }
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * Delete records in a batch(Transaction is managed in this method).
     *
     * <p>
     * This method will call commit automatically. If you want to manage transaction by yourself, please use bulkDeleteRecords
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param ids        the list of ids to delete
     * @return an array of CosmosDocument representing the number of rows deleted for each record
     * @throws Exception if a database error occurs
     */
    public static List<CosmosDocument> batchDeleteRecords(Connection conn, String schemaName, String tableName, List<String> ids) throws Exception {

        try {
            conn.setAutoCommit(false); // start transaction
            var ret = _deleteRecords(conn, schemaName, tableName, ids, true);

            conn.commit(); // do the commit
            return ret.successList;

        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                    log.warn("Transaction rolled back due to an error when batch deleting records from table '{}.{}'. records size:{}. ", schemaName, tableName, ids.size());
                }
            } catch (SQLException rollbackEx) {
                log.error("Failed to roll back transaction when batch deleting records from table '{}.{}'. records size:{}. ", schemaName, tableName, ids.size(), e);
            }
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }


    /**
     * Upsert records in a batch(Transaction is managed in this method).
     *
     * <p>
     * This method will call commit automatically. If you want to manage transaction by yourself, please use bulkUpsertRecords
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param records    the list of records to upsert
     * @return an array of CosmosDocument representing the number of rows upserted for each record
     * @throws Exception if a database error occurs
     */
    public static List<CosmosDocument> batchUpsertRecords(Connection conn, String schemaName, String tableName, List<PostgresRecord> records) throws Exception {

        try {
            conn.setAutoCommit(false); // start transaction
            var ret = _upsertRecords(conn, schemaName, tableName, records, true);

            conn.commit(); // do the commit
            return ret.successList;

        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                    log.warn("Transaction rolled back due to an error when batch upserting records into table '{}.{}'. records size:{}. ", schemaName, tableName, records.size());
                }
            } catch (SQLException rollbackEx) {
                log.error("Failed to roll back transaction when batch upserting records into table '{}.{}'. records size:{}. ", schemaName, tableName, records.size(), e);
            }
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }


    /**
     * bulk insert records into a table without transaction.
     *
     * <p>
     * This method will NOT begin a transaction. You can manage transaction by yourself, or you can use batchInsertRecords.
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param records    the list of records to insert
     * @return CosmosBulkResult instance including successList and fatalList
     * @throws Exception if a database error occurs
     */
    public static CosmosBulkResult bulkInsertRecords(Connection conn, String schemaName, String tableName, List<PostgresRecord> records) throws Exception {
        return _insertRecords(conn, schemaName, tableName, records, false);
    }

    /**
     * Bulk delete records(without transaction).
     *
     * <p>
     * This method will NOT begin a transaction. You can manage transaction by yourself, or you can use batchDeleteRecords
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param ids        the list of ids to delete
     * @return CosmosBulkResult instance including successList and fatalList
     * @throws Exception if a database error occurs
     */
    public static CosmosBulkResult bulkDeleteRecords(Connection conn, String schemaName, String tableName, List<String> ids) throws Exception {
        return _deleteRecords(conn, schemaName, tableName, ids, false);
    }

    /**
     * Bulk upsert records (without transaction).
     *
     * <p>
     * This method will NOT begin a transaction. You can manage transaction by yourself, or you can use batchUpsertRecords
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param records    the list of records to upsert
     * @return CosmosBulkResult instance including successList and fatalList
     * @throws Exception if a database error occurs
     */
    public static CosmosBulkResult bulkUpsertRecords(Connection conn, String schemaName, String tableName, List<PostgresRecord> records) throws Exception {
        return _upsertRecords(conn, schemaName, tableName, records, false);
    }

    /**
     * Inner method: Insert a bulk of records into a table(Transaction is NOT managed in this method).
     *
     * <p>
     * This method will call NOT commit automatically. You must manage transaction yourself. see also: batchInsertRecords
     * </p>
     *
     * @param conn           the database connection
     * @param schemaName     the name of the schema
     * @param tableName      the name of the table
     * @param records        the list of records to insert
     * @param throwException if true, throw exception if a database error occurs(used in batch). if false, record the results in ret and return it instead of throw an exception(used in bulk)
     * @return an array of integers representing the number of rows inserted for each record
     * @throws Exception if a database error occurs
     */
    public static CosmosBulkResult _insertRecords(Connection conn, String schemaName, String tableName, List<PostgresRecord> records, boolean throwException) throws Exception {

        Checker.checkNotNull(records, "records");

        var ret = new CosmosBulkResult();

        if (records.isEmpty()) {
            // do nothing if records is empty.
            return ret;
        }

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);


        // insert into table
        var insertSQL = String.format("""
                INSERT INTO %s.%s (%s, %s)
                VALUES (?, ?)
                """, schemaName, tableName, ID, DATA);

        try (var pstmt = conn.prepareStatement(insertSQL)) {

            var count = 0;
            var chunkSize = 100;

            // Add multiple records to the batch
            for (var record : records) {

                var id = record.id;
                var map = new HashMap<>(record.data);

                // add ID fields to map, for the compatibility with CosmosDB/MongoDB
                map.put(ID, id);
                var json = JsonUtil.toJson(map);


                pstmt.setString(1, id);
                pstmt.setObject(2, json, Types.OTHER);

                pstmt.addBatch();

                count++;

                // execute the batch in chunks, in order to reduce memory consumption
                if (count % chunkSize == 0) {
                    var expOccurred = executeAndRecordResult(schemaName, tableName, records, throwException, pstmt, chunkSize, count, ret);
                    if (expOccurred) {

                        // if exception occurred, do not execute the remained chunks. instead add all the remained records to fatalList.
                        // Then return and fail fast
                        for (var i = count; i < records.size(); i++) {
                            var docId = records.get(i).id;
                            ret.fatalList.add(new CosmosException(500, docId, "Skipped because database error occurred when batch inserting records into table '%s.%s'. id:%s, index:%d.".formatted(schemaName, tableName, docId, i)));
                        }
                        return ret;
                    }
                }

            }

            // Execute any remaining records in the batch
            executeAndRecordResult(schemaName, tableName, records, throwException, pstmt, chunkSize, count, ret);
            return ret;
        }
    }


    /**
     * Inner method: Bulk delete records (Transaction is NOT done in this method).
     *
     * <p>
     * This method will NOT call commit automatically. You must manage transaction your self. see also batchDeleteRecords
     * </p>
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param ids        the list of ids to delete
     * @param throwException if true, throw exception if a database error occurs(used in batch). if false, record the results in ret and return it instead of throw an exception(used in bulk)
     * @return an array of integers representing the number of rows affected for each record
     * @throws Exception if a database error occurs
     */
    public static CosmosBulkResult _deleteRecords(Connection conn, String schemaName, String tableName, List<String> ids, boolean throwException) throws Exception {

        Checker.checkNotNull(ids, "ids");

        var ret = new CosmosBulkResult();
        if (ids.isEmpty()) {
            // do nothing if records is empty.
            return ret;
        }

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);


        var deleteSQL = String.format("DELETE FROM %s.%s WHERE id = ?", schemaName, tableName);

        try (var pstmt = conn.prepareStatement(deleteSQL)) {

            int count = 0;
            int chunkSize = 100;
            for (var id : ids) {

                pstmt.setString(1, id);
                pstmt.addBatch();

                count++;

                // execute the batch in chunks, in order to reduce memory consumption
                if (count % chunkSize == 0) {
                    var expOccurred = executeDeletionAndRecordResult(schemaName, tableName, ids, throwException, pstmt, chunkSize, count, ret);

                    if (expOccurred) {

                        // if exception occurred, do not execute the remained chunks. instead add all the remained records to fatalList.
                        // Then return and fail fast
                        for (var i = count; i < ids.size(); i++) {
                            var docId = ids.get(i);
                            ret.fatalList.add(new CosmosException(500, docId, "Skipped because database error occurred when batch deleting records into table '%s.%s'. id:%s, index:%d.".formatted(schemaName, tableName, docId, i)));
                        }
                        return ret;

                    }
                }
            }

            // Execute any remaining records in the batch
            executeDeletionAndRecordResult(schemaName, tableName, ids, throwException, pstmt, chunkSize, count, ret);

            return ret;

        }
    }


    /**
     * Inner method: Upsert a bulk of records into a table(Transaction is NOT managed in this method).
     *
     * <p>
     * This method will call NOT commit automatically. You must manage transaction yourself. see also: batchUpsertRecords
     * </p>
     *
     * @param conn           the database connection
     * @param schemaName     the name of the schema
     * @param tableName      the name of the table
     * @param records        the list of records to upsert
     * @param throwException if true, throw exception if a database error occurs(used in batch). if false, record the results in ret and return it instead of throw an exception(used in bulk)
     * @return an array of integers representing the number of rows affected for each record
     * @throws Exception if a database error occurs
     */
    static CosmosBulkResult _upsertRecords(Connection conn, String schemaName, String tableName, List<PostgresRecord> records, boolean throwException) throws Exception {

        Checker.checkNotNull(records, "records");

        var ret = new CosmosBulkResult();
        if (records.isEmpty()) {
            // do nothing if records is empty.
            return ret;
        }

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        // upsert into table and return the result
        var upsertSQL = String.format("""
                INSERT INTO %s.%s (%s,%s)
                VALUES (?, ?)
                ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s
                """, schemaName, tableName, ID, DATA, ID, DATA, DATA);

        try (var pstmt = conn.prepareStatement(upsertSQL)) {

            int count = 0;
            int chunkSize = 100;

            for (var record : records) {

                checkValidRecord(record);

                pstmt.setString(1, record.id);
                pstmt.setObject(2, JsonUtil.toJson(record.data), Types.OTHER);
                pstmt.addBatch();

                count++;

                // execute the batch in chunks, in order to reduce memory consumption
                if (count % 100 == 0) {
                    var expOccurred = executeAndRecordResult(schemaName, tableName, records, throwException, pstmt, chunkSize, count, ret);
                    if (expOccurred) {

                        // if exception occurred, do not execute the remained chunks. instead add all the remained records to fatalList.
                        // Then return and fail fast
                        for (var i = count; i < records.size(); i++) {
                            var docId = records.get(i).id;
                            ret.fatalList.add(new CosmosException(500, docId, "Skipped because database error occurred when batch upserting records into table '%s.%s'. id:%s, index:%d.".formatted(schemaName, tableName, docId, i)));
                        }
                        return ret;

                    }
                }
            }
            // Execute any remaining records in the batch
            executeAndRecordResult(schemaName, tableName, records, throwException, pstmt, chunkSize, count, ret);

            return ret;

        }
    }

    /**
     * execute the batch in chunks, and record the results in cosmosBulkResult
     *
     * @param schemaName       the name of the schema
     * @param tableName        the name of the table
     * @param records          the list of records to insert/upsert
     * @param throwException   if true, throw exception if a database error occurs(used in batch). if false, record the results in ret and return it instead of throw an exception(used in bulk)
     * @param pstmt            the prepared statement
     * @param chunkSize        the size of each chunk
     * @param count            the current count of record
     * @param cosmosBulkResult the result of bulk operations
     * @return true/false whether SQLException occurred.
     * @throws SQLException if a database error occurs
     */
    static boolean executeAndRecordResult(String schemaName, String tableName, List<PostgresRecord> records, boolean throwException, PreparedStatement pstmt, int chunkSize, int count, CosmosBulkResult cosmosBulkResult) throws SQLException {

        var processedCount = cosmosBulkResult.successList.size() + cosmosBulkResult.fatalList.size() + cosmosBulkResult.retryList.size();
        if(processedCount >= records.size()) {
            // if processedCount is over record's size, no need to execute anymore
            return false;
        }
        // from index and to index for this chunk
        int from, to;
        if (count % chunkSize == 0) {
            // if this is a chunk that fit the chunkSize
            from = count - chunkSize;
            to = count;
        } else {
            // if this is a batch that not fit the chunkSize
            from = count - (count % chunkSize);
            to = count;
        }

        // size of this chunk
        var size = to - from;

        try {

            var part = pstmt.executeBatch();

            // record this chunk execution's result to cosmosBulkResult
            for (var i = 0; i < size; i++) {
                var docIndex = from + i;
                if (part[i] > 0) {
                    cosmosBulkResult.successList.add(getCosmosDocument(records.get(docIndex)));
                } else {
                    var docId = records.get(docIndex).id;
                    cosmosBulkResult.fatalList.add(new CosmosException(500, docId, "Failed to executeBulk. %s.%s, id:%s".formatted(schemaName, tableName, docId)));
                }
            }
            // no SQLException occurred
            return false;
        } catch (SQLException e) {
            // if throwException is true, throw the exception
            if (throwException) {
                throw e;
            } else {
                log.warn("Failed to executeBatch. {}.{}, from:{}, to:{}", schemaName, tableName, from, to, e);
                // record this batch execution's result to cosmosBulkResult
                for (var i = 0; i < size; i++) {
                    var docIndex = from + i;
                    var docId = records.get(docIndex).id;
                    cosmosBulkResult.fatalList.add(new CosmosException(500, docId, "Failed to executeBulk. %s.%s, id:%s, index:%s".formatted(schemaName, tableName, docId, docIndex), e));
                }
            }
            // SQLException occurred
            return true;
        }
    }

    /**
     * execute the batch in chunks, and record the results in cosmosBulkResult
     *
     * @param schemaName       the name of the schema
     * @param tableName        the name of the table
     * @param ids              the list of ids to delete
     * @param throwException   if true, throw exception if a database error occurs(used in batch). if false, record the results in ret and return it instead of throw an exception(used in bulk)
     * @param pstmt            the prepared statement
     * @param chunkSize        the size of each chunk
     * @param count            the current count of record
     * @param cosmosBulkResult the result of bulk operations
     * @return true/false whether SQLException occurred.
     * @throws SQLException if a database error occurs
     */
    static boolean executeDeletionAndRecordResult(String schemaName, String tableName, List<String> ids, boolean throwException, PreparedStatement pstmt, int chunkSize, int count, CosmosBulkResult cosmosBulkResult) throws SQLException {

        // from index and to index for this chunk
        int from, to;
        if (count % chunkSize == 0) {
            // if this is a chunk that fit the chunkSize
            from = count - chunkSize;
            to = count;
        } else {
            // if this is a batch that not fit the chunkSize
            from = count - (count % chunkSize);
            to = count;
        }

        // size of this chunk
        var size = to - from;

        try {

            pstmt.executeBatch();

            // record this chunk execution's result to cosmosBulkResult
            for (var i = 0; i < size; i++) {
                var docIndex = from + i;
                cosmosBulkResult.successList.add(new CosmosDocument(Map.of("id", ids.get(docIndex))));
            }
            // no SQLException occurred
            return false;
        } catch (SQLException e) {
            // if throwException is true, throw the exception
            if (throwException) {
                throw e;
            } else {
                log.warn("Failed to executeBatch. {}.{}, from:{}, to:{}", schemaName, tableName, from, to, e);
                // record this batch execution's result to cosmosBulkResult
                for (var i = 0; i < size; i++) {
                    var docIndex = from + i;
                    var docId = ids.get(docIndex);
                    cosmosBulkResult.fatalList.add(new CosmosException(500, docId, "Failed to executeDeletion. %s.%s, id:%s, index:%s".formatted(schemaName, tableName, docId, docIndex), e));
                }
            }
            // SQLException occurred
            return true;
        }
    }

    /**
     * convert PostgresRecord to CosmosDocument
     * @param record
     * @return
     */
    static CosmosDocument getCosmosDocument(PostgresRecord record) {

        var map = record.data;

        if(MapUtil.isImmutableMap(map)){
            map = Maps.newLinkedHashMap(record.data);
        }
        map.put(TableUtil.ID, record.id);
        return new CosmosDocument(map);
    }

    /**
     * aggregate records from a table with condition in a querySpec(queryText and params)
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param querySpec       the querySpec to find(including queryText and params)
     * @return aggregate results
     * @throws SQLException if a database error occurs
     */
    public static List<PostgresRecord> aggregateRecords(Connection conn, String schemaName, String tableName, CosmosSqlQuerySpec querySpec) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotNull(querySpec, "querySpec");
        Checker.checkNotBlank(querySpec.queryText, "querySpec.queryText");

        querySpec = NamedParameterUtil.convert(querySpec);

        var sql = querySpec.queryText;
        var params = querySpec.params;


        try (var pstmt = conn.prepareStatement(sql)) {

            setParamsForStatement(conn, params, pstmt);
            var ret = new ArrayList<PostgresRecord>();
            // Execute the query and return the result

            var rowNumber = 0;
            try (var resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    // Create a map to hold column names and their values for this row.
                    Map<String, Object> row = new HashMap<>();

                    // Retrieve the metadata from the ResultSet
                    var metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();

                    // Loop through all columns by index (starting at 1)
                    for (int i = 1; i <= columnCount; i++) {
                        // You can use getColumnName or getColumnLabel
                        String columnName = metaData.getColumnLabel(i);
                        Object value = resultSet.getObject(i);
                        row.put(columnName, value);
                    }

                    // Construct your PostgresRecord (adjust constructor as needed)
                    ret.add(new PostgresRecord(String.valueOf(rowNumber), row));
                    rowNumber++;
                }
                return ret;
            }
        } catch (SQLException e) {
            log.warn("Error when find records in table'{}.{}'. sql:{}", schemaName, tableName, querySpec.queryText, e);
            throw e;
        }
    }


    /**
     * find records from a table with condition in a querySpec(queryText and params)
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param querySpec       the querySpec to find(including queryText and params)
     * @return list of PostgresRecords
     * @throws SQLException if a database error occurs
     */
    public static List<PostgresRecord> findRecords(Connection conn, String schemaName, String tableName, CosmosSqlQuerySpec querySpec) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotNull(querySpec, "querySpec");
        Checker.checkNotBlank(querySpec.queryText, "querySpec.queryText");

        querySpec = NamedParameterUtil.convert(querySpec);

        var findSQL = querySpec.queryText;
        var params = querySpec.params;


        try (var pstmt = conn.prepareStatement(findSQL)) {

            setParamsForStatement(conn, params, pstmt);
            var ret = new ArrayList<PostgresRecord>();
            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
                while (resultSet.next()) {
                    ret.add(new PostgresRecord(resultSet.getString(ID), JsonUtil.toMap(resultSet.getString(DATA))));
                }
                return ret;
            }
        } catch (SQLException e) {
            log.warn("Error when find records in table'{}.{}'. sql:{}", schemaName, tableName, querySpec.queryText, e);
            throw e;
        }
    }

    /**
     * count records from a table with condition in a querySpec(queryText and params)
     *
     * @param conn       the database connection
     * @param schemaName the name of the schema
     * @param tableName  the name of the table
     * @param querySpec       the querySpec to find(including queryText and params)
     * @return count of records in int type
     * @throws SQLException if a database error occurs
     */
    public static int countRecords(Connection conn, String schemaName, String tableName, CosmosSqlQuerySpec querySpec) throws Exception {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);

        Checker.checkNotNull(querySpec, "querySpec");
        Checker.checkNotBlank(querySpec.queryText, "querySpec.queryText");


        querySpec = NamedParameterUtil.convert(querySpec);

        var findSQL = querySpec.queryText;
        var params = querySpec.params;


        try (var pstmt = conn.prepareStatement(findSQL)) {

            setParamsForStatement(conn, params, pstmt);
            var ret = new ArrayList<PostgresRecord>();
            // Execute the query and return the result
            try (var resultSet = pstmt.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt(1);
                }
                return 0;
            }
        } catch (SQLException e) {
            log.warn("Error when count records in table'{}.{}'. sql:{}", schemaName, tableName, querySpec.queryText, e);
            throw e;
        }
    }


    /**
     * Checks if an index exists in the specified schema.
     *
     * @param conn       the database connection
     * @param schemaName the schema name
     * @param tableName  the table name
     * @param fieldName  the index name
     * @return true if the index exists, false otherwise
     * @throws SQLException if a database error occurs
     */
    static boolean indexExists(Connection conn, String schemaName, String tableName, String fieldName) throws SQLException {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);
        var indexName = getIndexName(tableName, fieldName);

        return indexExistsByName(conn, schemaName, tableName, indexName);
    }


    /**
     * Checks if an index exists in the specified schema.
     *
     * @param conn       the database connection
     * @param schemaName the schema name
     * @param tableName  the table name
     * @param indexName  the index name
     * @return true if the index exists, false otherwise
     * @throws SQLException if a database error occurs
     */
    static boolean indexExistsByName(Connection conn, String schemaName, String tableName, String indexName) throws SQLException {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);
        indexName = checkAndNormalizeValidEntityName(indexName);

        schemaName = removeQuotes(schemaName);
        tableName = removeQuotes(tableName);
        indexName = removeQuotes(indexName);

        // Query pg_indexes to check for the index existence.
        var query = "SELECT 1 FROM pg_indexes WHERE schemaname = ? AND tablename = ? AND indexname = ?";
        try (var pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, schemaName);
            pstmt.setString(2, tableName);
            pstmt.setString(3, indexName);
            try (var rs = pstmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * remove starting and ending quotes
     * @param entityName
     * @return
     */
    public static String removeQuotes(String entityName) {
        entityName = StringUtils.removeStart(entityName,"\"");
        return StringUtils.removeEnd(entityName,"\"");
    }

    /**
     * Drops the specified index if it exists.
     *
     * @param conn the JDBC connection
     * @param schemaName the schema name
     * @param tableName  the table name for index
     * @param fieldName  the field name for index
     * @throws SQLException if a database access error occurs
     */
    static void dropIndexIfExists(Connection conn, String schemaName, String tableName, String fieldName) throws SQLException {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        var indexName = getIndexName(tableName, fieldName);

        dropIndexIfExists(conn, schemaName, indexName);
    }


    /**
     * Drops the specified index if it exists.
     *
     * @param connection the JDBC connection
     * @param schemaName the schema name
     * @param indexName  the index name to drop
     * @throws SQLException if a database access error occurs
     */
    static void dropIndexIfExists(Connection connection, String schemaName, String indexName) throws SQLException {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        indexName = checkAndNormalizeValidEntityName(indexName);

        // postgres supports the "DROP INDEX IF EXISTS" syntax.
        String sql = "DROP INDEX IF EXISTS " + schemaName + "." + indexName;
        try (var stmt = connection.createStatement()) {
            stmt.execute(sql);
            if (log.isInfoEnabled()) {
                log.info("Index '{}.{}' dropped successfully.", schemaName, indexName);
            }
        }
    }



    /**
     * Checks if a schema exists in the database.
     *
     * @param conn       the database connection
     * @param schemaName the schema name to check
     * @return true if the schema exists, false otherwise
     * @throws SQLException if a database error occurs
     */
    public static boolean schemaExists(Connection conn, String schemaName) throws SQLException {
        // Optionally, normalize or validate the schema name as needed
        schemaName = checkAndNormalizeValidEntityName(schemaName);

        var expectedSchemaName = removeQuotes(schemaName);
        var metaData = conn.getMetaData();
        try (var schemas = metaData.getSchemas()) {
            while (schemas.next()) {
                var existingSchema = schemas.getString("TABLE_SCHEM");
                if (expectedSchemaName.equals(existingSchema)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Creates an index with the specified schemaName, tableName and fieldName if it does not already exist.
     * Only index under "data" column is allowed.
     *
     * <p>
     * Index can be created on nested json path under "data" column.
     * For example, to create a unique index on `lastName` field under `data` column:
     * </p>
     * <pre>
     *     {@code
     *
     *         // input:
     *         // schemaName : schema1
     *         // tableName : table1
     *         // fieldName : address.city.street
     *         // IndexOption: {unique: true}
     *
     *         // index name will be automatically generated from schemaName, tableName and fieldName
     *
     *         CREATE UNIQUE INDEX table1_address_city_street_1 ON schema1.table1 ((data->'address'->'city'->>'street'));
     *     }
     * </pre>
     *
     *
     * @param conn       the database connection
     * @param schemaName the schema name
     * @param tableName  the table name
     * @param fieldName  the field name representing a JSON path. e.g.   address.city.street
     * @param indexOption     the index options
     * @return schema.indexName if created, or "" if index already exists
     * @throws SQLException if a database error occurs
     */
    public static String createIndexIfNotExists(Connection conn, String schemaName, String tableName, String fieldName, IndexOption indexOption) throws SQLException {

        schemaName = checkAndNormalizeValidEntityName(schemaName);
        tableName = checkAndNormalizeValidEntityName(tableName);
        // fieldName only checked. no need to normalize.
        // because we will need to use the origin fieldName in index creation SQL
        checkValidEntityName(fieldName);

        // Generate index name from table name and field name
        // e.g.  idx_table1_address_city_street_1
        var indexName = getIndexName(tableName, fieldName);

        if (indexExistsByName(conn, schemaName, tableName, indexName)) {
            // already exists
            return "";
        }

        // create index

        // Construct JSON path expression
        // data->'address'->'city'->>'street'
        var jsonPathExpression = PGKeyUtil.getFormattedKey(fieldName);

        var createIndexSQL = """
                CREATE %s INDEX %s
                  ON %s.%s ((%s));
                """
                .formatted(indexOption.unique ? "UNIQUE" : "", indexName, schemaName, tableName, jsonPathExpression);

        try (var pstmt = conn.prepareStatement(createIndexSQL)) {
            pstmt.executeUpdate();
            if (log.isInfoEnabled()) {
                log.info("Index({}) on column '{}' with field '{}' of table '{}.{}' created successfully.", indexName, DATA, fieldName, schemaName, tableName);
            }
        }

        return "%s.%s".formatted(schemaName, indexName);
    }

    /**
     * Generate indexName from table name and field name
     * @param tableName
     * @param fieldName
     * @return indexName e.g. table1_address_city_street_1
     */
    static String getIndexName(String tableName, String fieldName) {

        fieldName = fieldName
                .replace(".", "_")
                .replace("-", "_")
        ;

        checkValidEntityName(tableName);
        checkValidEntityName(fieldName);

        tableName = removeQuotes(tableName);

        var indexName =  String.format("idx_%s_%s_1", tableName, fieldName);

        return checkAndNormalizeValidEntityName(indexName);

    }

    /**
     * Sets the parameters for the given {@link PreparedStatement} with the given
     * parameters. The parameters are expected to be in the same order as the
     * parameter placeholders in the SQL query string.
     *
     * @param conn  the database connection
     * @param params the list of parameters
     * @param pstmt  the prepared statement
     * @throws SQLException if a database error occurs
     */
    static void setParamsForStatement(Connection conn, List<CosmosSqlParameter> params, PreparedStatement pstmt) throws SQLException {
        var index = 1;
        for (var param : params) {
            if (param.value instanceof String strValue) {
                pstmt.setString(index, strValue);
            } else if (param.value instanceof Long longValue) {
                pstmt.setLong(index, longValue);
            } else if (param.value instanceof Integer intValue) {
                pstmt.setInt(index, intValue);
            } else if (param.value instanceof Boolean boolValue) {
                pstmt.setBoolean(index, boolValue);
            } else if (param.value instanceof Float floatValue) {
                pstmt.setFloat(index, floatValue);
            } else if (param.value instanceof Double doubleValue) {
                pstmt.setDouble(index, doubleValue);
            } else if (param.value instanceof BigDecimal bigDecimalValue) {
                pstmt.setBigDecimal(index, bigDecimalValue);
            } else if (param.value instanceof Collection<?> collectionValue) {
                // Determine the SQL type of the array elements
                var sqlType = getSqlType(collectionValue);
                // Create the java.sql.Array instance
                var sqlArray = conn.createArrayOf(sqlType, collectionValue.toArray());
                pstmt.setArray(index, sqlArray);
            } else {
                pstmt.setString(index, JsonUtil.toJson(param.value));
            }
            index++;
        }
    }

    /**
     * Returns the SQL type that corresponds to the type of elements in the given collection.
     *
     * If the collection is empty, returns "text" as a default.
     *
     * @param collection the collection to determine the SQL type for
     * @return the corresponding SQL type
     */
    static String getSqlType(Collection<?> collection) {
        if (collection.isEmpty()) {
            return "text"; // default to varchar if empty
        }
        var firstElement = collection.iterator().next();
        if (firstElement instanceof String) return "text";
        if (firstElement instanceof Integer) return "integer";
        if (firstElement instanceof Long) return "bigint";
        if (firstElement instanceof Double) return "double precision";
        // Add more types as needed
        return "text"; // default to varchar
    }

    /**
     * Checks if the given name is suitable for a postgres table name.
     *
     * A valid postgres table name, according to the requirements, should:
     * - Consist of lowercase alphanumeric characters (a-z, 0-9).
     * - Allow underscores (_).
     * - Not be empty or null.
     *
     * @param name The name to check.
     * @return {@code true} if the name is a valid postgres table name, {@code false} otherwise.
     */
    static boolean isSimpleEntityName(String name) {
        if (name == null || name.isEmpty()) {
            return false; // Null or empty names are not valid
        }

        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!isSimpleEntityChar(c)) {
                return false; // Found an invalid character
            }
        }
        return true; // All characters are valid
    }

    private static boolean isSimpleEntityChar(char c) {
        return (c >= 'a' && c <= 'z') || // lowercase letters
                (c >= '0' && c <= '9') || // digits
                (c == '_');              // underscore
    }


}
