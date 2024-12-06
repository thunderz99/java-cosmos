package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
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
                assertThat(inserted).isNotNull();
                assertThat(inserted.id).isEqualTo(id);
                assertThat(inserted.tenantId).isEqualTo(tenantId);
                assertThat(inserted.data.get("id")).isEqualTo(id);
                assertThat(inserted.data.get("name")).isEqualTo("John Doe");


                //read record
                var read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNotNull();
                assertThat(read.id).isEqualTo(id);
                assertThat(read.tenantId).isEqualTo(tenantId);
                assertThat(read.data.get("id")).isEqualTo(id);
                assertThat(read.data.get("name")).isEqualTo("John Doe");

                //update record
                Map<String, Object> updatedData = Map.of("name", "Jane Doe");
                var updated = TableUtil.updateRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, updatedData));
                assertThat(updated).isNotNull();
                assertThat(updated.id).isEqualTo(id);
                assertThat(updated.tenantId).isEqualTo(tenantId);
                assertThat(updated.data.get("id")).isEqualTo(id);
                assertThat(updated.data.get("name")).isEqualTo("Jane Doe");


                //read record
                read = TableUtil.readRecord(conn, schemaName, tableName, id);
                assertThat(read).isNotNull();
                assertThat(read.id).isEqualTo(id);
                assertThat(read.tenantId).isEqualTo(tenantId);
                assertThat(read.data.get("id")).isEqualTo(id);
                assertThat(read.data.get("name")).isEqualTo("Jane Doe");

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
                    var upserted = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, data));
                    assertThat(upserted).isNotNull();
                    assertThat(upserted.id).isEqualTo(id);
                    assertThat(upserted.tenantId).isEqualTo(tenantId);
                    assertThat(upserted.data.get("id")).isEqualTo(id);
                    assertThat(upserted.data.get("name")).isEqualTo("John Doe");

                    // read record
                    var read = TableUtil.readRecord(conn, schemaName, tableName, id);
                    assertThat(read).isNotNull();
                    assertThat(read.id).isEqualTo(id);
                    assertThat(read.tenantId).isEqualTo(tenantId);
                    assertThat(read.data.get("id")).isEqualTo(id);
                    assertThat(read.data.get("name")).isEqualTo("John Doe");

                    // update record using upsertRecord
                    Map<String, Object> updatedData = Map.of("name", "Jane Doe");
                    var updated = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, updatedData));

                    // read record
                    read = TableUtil.readRecord(conn, schemaName, tableName, id);
                    assertThat(read).isNotNull();
                    assertThat(read.id).isEqualTo(id);
                    assertThat(read.tenantId).isEqualTo(tenantId);
                    assertThat(read.data.get("id")).isEqualTo(id);
                    assertThat(read.data.get("name")).isEqualTo("Jane Doe");
                }

                {
                    // missing id
                    assertThatThrownBy(() -> TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(null, tenantId, data)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.id should be non-empty")
                    ;
                }

                {
                    // missing tenantId
                    assertThatThrownBy(() -> TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, null, data)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.tenantId should not be null")
                    ;
                }

                {
                    // missing data
                    assertThatThrownBy(() -> TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, null)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.map should not be null")
                    ;
                }

                {
                    // when data is empty
                    var upserted = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, Map.of()));
                    assertThat(upserted.id).isEqualTo(id);
                    assertThat(upserted.tenantId).isEqualTo(tenantId);

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
                TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, data));

                // normal update partial
                {
                    // normal case
                    Map<String, Object> updatedData = Map.of("age", 25, "address", "CA");
                    var updated = TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, updatedData));
                    assertThat(updated).isNotNull();
                    assertThat(updated.id).isEqualTo(id);
                    assertThat(updated.tenantId).isEqualTo(tenantId);
                    assertThat(updated.data.get("id")).isEqualTo(id);
                    assertThat(updated.data.get("name")).isEqualTo("Tom Partial"); // not updated
                    assertThat(updated.data.get("age")).isEqualTo(25); // updated
                    assertThat(updated.data.get("address")).isEqualTo("CA"); // updated
                }

                {
                    // not exist id
                    Map<String, Object> updatedData = Map.of("age", 30, "address", "TX");
                    assertThatThrownBy(() -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord("not_exist_id", tenantId, updatedData)))
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
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(null, tenantId, updatedData)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.id should be non-empty");
                }
                {
                    // tenantId is null
                    assertThatThrownBy(
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, null, Map.of("age", 25, "address", "CA"))))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("record.tenantId should not be null");
                }
                {
                    assertThatThrownBy(
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, null)))
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
        var tenantId = "Data_patchRecord_" + RandomStringUtils.randomAlphanumeric(6);

        try {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Map.of("id", id, "name", "John Doe", "age", 23);
                TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, tenantId, data));

                // normal case
                {
                    var operations = PatchOperations.create()
                            .set("/age", 24)
                            //.add("/address/city", "NY"); TODO nested fields
                            .add("/address", "NY");
                    var patched = TableUtil.patchRecord(conn, schemaName, tableName, tenantId, id, operations);
                    assertThat(patched).isNotNull();
                    assertThat(patched.id).isEqualTo(id);
                    assertThat(patched.tenantId).isEqualTo(tenantId);
                    assertThat(patched.data.get("id")).isEqualTo(id);
                    assertThat(patched.data.get("name")).isEqualTo("John Doe"); // not updated
                    assertThat(patched.data.get("age")).isEqualTo(24); // updated
                    assertThat(patched.data.get("address")).isEqualTo("NY"); // added
                }

                // irregular input values
                {
                    // id is null
                    var operations = PatchOperations.create()
                            .set("/age", 25);
                    assertThatThrownBy(
                            () -> TableUtil.patchRecord(conn, schemaName, tableName, tenantId, null, operations))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("id should be non-empty");
                }
                {
                    // tenantId is null
                    var operations = PatchOperations.create()
                            .set("/age", 26);
                    assertThatThrownBy(
                            () -> TableUtil.patchRecord(conn, schemaName, tableName, null,id, operations))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("tenantId should not be null");
                }
                {
                    // operations are null
                    assertThatThrownBy(
                            () -> TableUtil.patchRecord(conn, schemaName, tableName, tenantId, id, null))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("operations should not be null");
                }
                {
                    // operations are empty
                    var operations = PatchOperations.create();
                    var record = TableUtil.patchRecord(conn, schemaName, tableName, tenantId, id, operations);
                    assertThat(record.id).isEqualTo(id);
                    assertThat(record.tenantId).isEqualTo(tenantId);
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