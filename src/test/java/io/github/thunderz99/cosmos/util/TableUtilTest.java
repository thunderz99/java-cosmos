package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImplTest;
import io.github.thunderz99.cosmos.impl.postgres.PostgresRecord;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableUtilTest {

    static PostgresImpl cosmos;

    static final String dbName = "java_cosmos";
    static final String schemaName = "table_util_test_schema_" + StringUtils.lowerCase(RandomStringUtils.randomAlphanumeric(6));

    @BeforeAll
    static void beforeAll() throws Exception {
        cosmos = new PostgresImpl(EnvUtil.getOrDefault("POSTGRES_CONNECTION_STRING", PostgresImplTest.LOCAL_CONNECTION_STRING));
        cosmos.createIfNotExist(dbName, schemaName);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if(cosmos != null) {
            cosmos.deleteCollection(dbName, schemaName);
            cosmos.closeClient();
        }
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

        try (var conn = cosmos.getDataSource().getConnection()){
            {

                //create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                //insert record
                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Map.of("id", id, "name", "John Doe");
                var inserted = TableUtil.insertRecord(conn, schemaName, tableName, new PostgresRecord(id, data));
                assertThat(inserted).isNotNull();
                assertThat(inserted.id).isEqualTo(id);
                assertThat(inserted.data.get("id")).isEqualTo(id);
                assertThat(inserted.data.get("name")).isEqualTo("John Doe");


                //read record
                var read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNotNull();
                assertThat(read.id).isEqualTo(id);
                assertThat(read.data.get("id")).isEqualTo(id);
                assertThat(read.data.get("name")).isEqualTo("John Doe");

                //update record
                Map<String, Object> updatedData = Map.of("name", "Jane Doe");
                var updated = TableUtil.updateRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData));
                assertThat(updated).isNotNull();
                assertThat(updated.id).isEqualTo(id);
                assertThat(updated.data.get("id")).isEqualTo(id);
                assertThat(updated.data.get("name")).isEqualTo("Jane Doe");


                //read record
                read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNotNull();
                assertThat(read.id).isEqualTo(id);
                assertThat(read.data.get("id")).isEqualTo(id);
                assertThat(read.data.get("name")).isEqualTo("Jane Doe");

                //delete record
                TableUtil.deleteRecord(conn, schemaName, tableName, id);

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


    @Test
    void upsert_should_work() throws Exception {

        var tableName = "upsert_test_" + RandomStringUtils.randomAlphanumeric(6);

        var tenantId = "Data_upsert_" + RandomStringUtils.randomAlphanumeric(6);

        try (var conn = cosmos.getDataSource().getConnection()){
            {

                // create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Map.of("id", id, "name", "John Doe");

                {
                    // insert record using upsertRecord
                    var upserted = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, data));
                    assertThat(upserted).isNotNull();
                    assertThat(upserted.id).isEqualTo(id);
                    assertThat(upserted.data.get("id")).isEqualTo(id);
                    assertThat(upserted.data.get("name")).isEqualTo("John Doe");

                    // read record
                    var read = TableUtil.readRecord(conn, schemaName, tableName, id);
                    assertThat(read).isNotNull();
                    assertThat(read.id).isEqualTo(id);
                    assertThat(read.data.get("id")).isEqualTo(id);
                    assertThat(read.data.get("name")).isEqualTo("John Doe");

                    // update record using upsertRecord
                    Map<String, Object> updatedData = Map.of("name", "Jane Doe");
                    var updated = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData));

                    // read record
                    read = TableUtil.readRecord(conn, schemaName, tableName, id);
                    assertThat(read).isNotNull();
                    assertThat(read.id).isEqualTo(id);
                    assertThat(read.data.get("id")).isEqualTo(id);
                    assertThat(read.data.get("name")).isEqualTo("Jane Doe");
                }

                {
                    // missing id
                    assertThatThrownBy(() -> TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(null, data)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.id should be non-empty")
                    ;
                }

                {
                    // missing data
                    assertThatThrownBy(() -> TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, null)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.map should not be null")
                    ;
                }

                {
                    // when data is empty
                    var upserted = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, Map.of()));
                    assertThat(upserted.id).isEqualTo(id);

                    // even if data is empty, at lease the "id" field will be added to the data map
                    assertThat(upserted.data).hasSize(1).containsKey("id");
                }
            }

        } finally {
            try(var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void updatePartial_should_work() throws Exception {

        var tableName = "upsert_partial_test_" + RandomStringUtils.randomAlphanumeric(6);
        var tenantId = "Data_upsertPartial_" + RandomStringUtils.randomAlphanumeric(6);

        try {
            try(var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Map.of("id", id, "name", "Tom Partial", "address", "NY");
                TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, data));

                // normal update partial
                {
                    // normal case
                    Map<String, Object> updatedData = Map.of("age", 25, "address", "CA");
                    var updated = TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData));
                    assertThat(updated).isNotNull();
                    assertThat(updated.id).isEqualTo(id);
                    assertThat(updated.data.get("id")).isEqualTo(id);
                    assertThat(updated.data.get("name")).isEqualTo("Tom Partial"); // not updated
                    assertThat(updated.data.get("age")).isEqualTo(25); // updated
                    assertThat(updated.data.get("address")).isEqualTo("CA"); // updated
                }

                {
                    // not exist id
                    Map<String, Object> updatedData = Map.of("age", 30, "address", "TX");
                    assertThatThrownBy(() -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord("not_exist_id", updatedData)))
                            .isInstanceOfSatisfying(CosmosException.class, e -> {
                                assertThat(e.getStatusCode()).isEqualTo(404);
                            })
                            .hasMessageContaining("Not Found");
                }

                // irregular input values
                {
                    // missing id
                    Map<String, Object> updatedData = Map.of("age", 25, "address", "CA");
                    assertThatThrownBy(
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(null, updatedData)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.id should be non-empty");
                }
                {
                    assertThatThrownBy(
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, null)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("record.map should not be null");
                }
            }


        } finally {
            try(var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }


    @Test
    void patchRecord_should_work() throws Exception {

        var tableName = "patchRecord_test_" + RandomStringUtils.randomAlphanumeric(6);

        try {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Map.of("id", id, "name", "John Doe", "age", 23);
                TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, data));

                // normal case
                {
                    var operations = PatchOperations.create()
                            .set("/age", 24)
                            .add("/address/city", "NY") // nested fields
                    ;
                    var patched = TableUtil.patchRecord(conn, schemaName, tableName, id, operations);
                    assertThat(patched).isNotNull();
                    assertThat(patched.id).isEqualTo(id);
                    assertThat(patched.data.get("id")).isEqualTo(id);
                    assertThat(patched.data.get("name")).isEqualTo("John Doe"); // not updated
                    assertThat(patched.data.get("age")).isEqualTo(24); // updated
                    assertThat((Map<String, Object>)patched.data.get("address")).containsEntry("city", "NY"); // added
                }

                // irregular input values
                {
                    // id is null
                    var operations = PatchOperations.create()
                            .set("/age", 25);
                    assertThatThrownBy(
                            () -> TableUtil.patchRecord(conn, schemaName, tableName, null, operations))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("id should be non-empty");
                }
                {
                    // operations are null
                    assertThatThrownBy(
                            () -> TableUtil.patchRecord(conn, schemaName, tableName, id, null))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("operations should not be null");
                }
                {
                    // operations are empty
                    var operations = PatchOperations.create();
                    var record = TableUtil.patchRecord(conn, schemaName, tableName, id, operations);
                    assertThat(record.id).isEqualTo(id);
                    assertThat(record.data).isEmpty();
                }
            }
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }
}