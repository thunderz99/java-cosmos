package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class TableUtilTest {

    static PostgresImpl cosmos;

    static final String schemaName = "table_util_test_schema_" + StringUtils.lowerCase(RandomStringUtils.randomAlphanumeric(6));

    @BeforeAll
    static void beforeAll() throws Exception {
        cosmos = new PostgresImpl(EnvUtil.get("POSTGRES_CONNECTION_STRING"));
        cosmos.createIfNotExist(schemaName, "");
    }

    @AfterAll
    static void afterAll() throws Exception {
        cosmos.deleteDatabase(schemaName);
        cosmos.closeClient();
    }

    @Test
    void createTableIfNotExist_should_work() throws Exception {


        var tableName = "create_table_if_not_exist_" + RandomStringUtils.randomAlphanumeric(6);

        try (var conn = cosmos.getDataSource().getConnection()){
            {

                //table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);
                assertThat(tableExist).isFalse();
            }
            {
                //create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                //table exists
                var conn2 = cosmos.getDataSource().getConnection();
                var tableExist = TableUtil.tableExist(conn2, schemaName, tableName);
                assertThat(tableExist).isTrue();
            }

            {
                //drop table
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
                //table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);

                assertThat(tableExist).isFalse();
            }

        } finally {
            try(var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void crud_should_work() throws Exception {

        var tableName = "crud_test_" + RandomStringUtils.randomAlphanumeric(6);

        var tenantId = "Data_crud_" + RandomStringUtils.randomAlphanumeric(6);

        try (var conn = cosmos.getDataSource().getConnection()){
            {

                //create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                //insert record
                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Map.of("id", id, "name", "John Doe");
                var inserted = TableUtil.insertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, data));

                //read record
                var read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNotNull();
                assertThat(read.id).isEqualTo(id);
                assertThat(read.tenantId).isEqualTo(tenantId);
                assertThat(read.map.get("id")).isEqualTo(id);
                assertThat(read.map.get("name")).isEqualTo("John Doe");

                //update record
                Map<String, Object> updatedData = Map.of("name", "Jane Doe");
                var updated = TableUtil.updateRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, updatedData));


                //read record
                read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNotNull();
                assertThat(read.id).isEqualTo(id);
                assertThat(read.tenantId).isEqualTo(tenantId);
                assertThat(read.map.get("id")).isEqualTo(id);
                assertThat(read.map.get("name")).isEqualTo("Jane Doe");

                //delete record
                TableUtil.deleteRecord(conn, schemaName, tableName, tenantId, id);

                //read record
                read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNull();
            }


        } finally {
            try(var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }

}