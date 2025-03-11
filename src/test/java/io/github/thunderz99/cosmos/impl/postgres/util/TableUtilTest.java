package io.github.thunderz99.cosmos.impl.postgres.util;

import com.google.common.collect.Maps;
import io.github.thunderz99.cosmos.CosmosException;
import io.github.thunderz99.cosmos.dto.CosmosSqlParameter;
import io.github.thunderz99.cosmos.dto.CosmosSqlQuerySpec;
import io.github.thunderz99.cosmos.dto.PartialUpdateOption;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImplTest;
import io.github.thunderz99.cosmos.impl.postgres.PostgresRecord;
import io.github.thunderz99.cosmos.impl.postgres.dto.IndexOption;
import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.v4.PatchOperations;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableUtilTest {

    static PostgresImpl cosmos;

    static final String dbName = "java_cosmos";
    static final String schemaName = "table_util_test_schema_" + RandomStringUtils.randomAlphanumeric(6).toUpperCase();
    static final String formattedSchemaName = TableUtil.checkAndNormalizeValidEntityName(schemaName);

    @BeforeAll
    static void beforeAll() throws Exception {
        cosmos = new PostgresImpl(EnvUtil.getOrDefault("POSTGRES_CONNECTION_STRING", PostgresImplTest.LOCAL_CONNECTION_STRING));
        cosmos.createIfNotExist(dbName, schemaName);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (cosmos != null) {
            cosmos.deleteCollection(dbName, schemaName);
            cosmos.closeClient();
        }
    }

    @Test
    void createTableIfNotExist_should_work() throws Exception {


        var tableName = "create_table_if_not_exist_" + RandomStringUtils.randomAlphanumeric(6).toUpperCase();
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);

        try (var conn = cosmos.getDataSource().getConnection()) {
            {

                // table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);
                assertThat(tableExist).isFalse();
            }
            {
                // create table
                var created = TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                assertThat(created).isEqualTo( formattedSchemaName + "." + formattedTableName);

                // table exists
                try(var conn2 = cosmos.getDataSource().getConnection()){
                    var tableExist = TableUtil.tableExist(conn2, schemaName, tableName);
                    assertThat(tableExist).isTrue();
                }

                // create table the second time. and it should be no-op
                var created2 = TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                assertThat(created2).isEmpty();
            }

            {
                //drop table
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
                //table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);

                assertThat(tableExist).isFalse();
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void createTableIfNotExist_should_work_4_uuid() throws Exception {


        var tableName = "create_table_if_not_exist_" + UUID.randomUUID().toString();

        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);

        try (var conn = cosmos.getDataSource().getConnection()) {
            {

                // table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);
                assertThat(tableExist).isFalse();
            }
            {
                // create table
                var created = TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                assertThat(created).isEqualTo( formattedSchemaName + "." + formattedTableName);

                // table exists
                try(var conn2 = cosmos.getDataSource().getConnection()){
                    var tableExist = TableUtil.tableExist(conn2, schemaName, tableName);
                    assertThat(tableExist).isTrue();
                }

                // create table the second time. and it should be no-op
                var created2 = TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                assertThat(created2).isEmpty();
            }

            {
                //drop table
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
                //table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);

                assertThat(tableExist).isFalse();
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void crud_should_work() throws Exception {

        var tableName = "crud_test_" + RandomStringUtils.randomAlphanumeric(6).toUpperCase();

        try (var conn = cosmos.getDataSource().getConnection()) {
            {

                //create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                //insert record
                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Maps.newHashMap(Map.of("id", id, "name", "John Doe"));
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
                Map<String, Object> updatedData = Maps.newHashMap(Map.of("name", "Jane Doe"));
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
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }

    @Test
    void crud_should_work_4_long_table_names() throws Exception {

        //long table name (> 63 chars)
        var tableName = "crud_test_" + RandomStringUtils.randomAlphanumeric(64);

        try (var conn = cosmos.getDataSource().getConnection()) {
            {

                //create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                //insert record
                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Maps.newHashMap(Map.of("id", id, "name", "John Doe"));
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
                Map<String, Object> updatedData = Maps.newHashMap(Map.of("name", "Jane Doe"));
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
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void upsert_should_work() throws Exception {

        var tableName = "upsert_test_" + RandomStringUtils.randomAlphanumeric(6);

        try (var conn = cosmos.getDataSource().getConnection()) {
            {

                // create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Maps.newHashMap(Map.of("id", id, "name", "John Doe"));

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
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("name", "Jane Doe"));
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
                            .hasMessageContaining("data should not be null")
                    ;
                }

                {
                    // when data is empty
                    var upserted = TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, Maps.newHashMap()));
                    assertThat(upserted.id).isEqualTo(id);

                    // even if data is empty, at lease the "id" field will be added to the data map
                    assertThat(upserted.data).hasSize(1).containsKey("id");
                }
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void updatePartial_should_work() throws Exception {

        var tableName = "upsert_partial_test_" + RandomStringUtils.randomAlphanumeric(6);
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);

        try {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var id = RandomStringUtils.randomAlphanumeric(6);
                Map<String, Object> data = Maps.newHashMap(Map.of("id", id, "name", "Tom Partial", "address", "NY", TableUtil.ETAG, RandomStringUtils.randomAlphanumeric(6)));
                TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, data));

                // normal update partial
                {
                    // normal case
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 25, "address", "CA"));
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
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 30, "address", "TX"));
                    assertThatThrownBy(() -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord("not_exist_id", updatedData)))
                            .isInstanceOfSatisfying(CosmosException.class, e -> {
                                assertThat(e.getStatusCode()).isEqualTo(404);
                            })
                            .hasMessageContaining("Not Found");
                }

                // irregular input values
                {
                    // missing id
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 25, "address", "CA"));
                    assertThatThrownBy(
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(null, updatedData)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("record.id should be non-empty");
                }
                {
                    assertThatThrownBy(
                            () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, null)))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("data should not be null");
                }


                {
                    // with option.checkEtag = true, and with normal etag
                    var record = TableUtil.readRecord(conn, schemaName, tableName, id);
                    var etag = record.data.getOrDefault(TableUtil.ETAG, "").toString();
                    assertThat(etag).isNotEmpty();
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 26, "address", "FL"));
                    var option = PartialUpdateOption.checkETag(true);
                    var updated = TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData), option, etag);
                    assertThat(updated).isNotNull();
                    assertThat(updated.id).isEqualTo(id);
                    assertThat(updated.data.get("id")).isEqualTo(id);
                    assertThat(updated.data.get("name")).isEqualTo("Tom Partial"); // not updated
                    assertThat(updated.data.get("age")).isEqualTo(26); // updated
                    assertThat(updated.data.get("address")).isEqualTo("FL"); // updated
                }

                {
                    // with option.checkEtag = true, and with etag = ""
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 26, "address", "WS"));
                    var option = PartialUpdateOption.checkETag(true);
                    var updated = TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData), option, "");
                    assertThat(updated).isNotNull();
                    assertThat(updated.id).isEqualTo(id);
                    assertThat(updated.data.get("address")).isEqualTo("WS"); // updated
                }

                {
                    // with option.checkEtag = false, and with etag = "not correct tag"
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 26, "address", "AB"));
                    var option = PartialUpdateOption.checkETag(false);
                    var updated = TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData), option, "not correct tag");
                    assertThat(updated).isNotNull();
                    assertThat(updated.id).isEqualTo(id);
                    assertThat(updated.data.get("address")).isEqualTo("AB"); // updated
                }

                {
                    // with option.checkEtag = true, and with etag = "not correct tag"
                    Map<String, Object> updatedData = Maps.newHashMap(Map.of("age", 26, "address", "LA"));
                    var option = PartialUpdateOption.checkETag(true);

                    assertThatThrownBy( () -> TableUtil.updatePartialRecord(conn, schemaName, tableName, new PostgresRecord(id, updatedData), option, "not correct tag"))
                            .isInstanceOfSatisfying(CosmosException.class, e -> {
                                assertThat(e.getStatusCode()).isEqualTo(412);
                                assertThat(e.getCode()).isEqualTo("412 Precondition Failed");
                                assertThat(e.getMessage()).contains("table:'%s.%s', id:%s".formatted(formattedSchemaName, formattedTableName, id));
                            });
                }


            }


        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
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
                Map<String, Object> data = Maps.newHashMap(Map.of("id", id, "name", "John Doe", "age", 23));
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
                    assertThat((Map<String, Object>) patched.data.get("address")).containsEntry("city", "NY"); // added
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
                    // only contains a map of id
                    assertThat(record.data).hasSize(1).containsKey("id");
                }
            }
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }

    @Test
    void batchInsertRecords_should_work() throws Exception {
        var tableName = "batchInsertRecords_test_" + RandomStringUtils.randomAlphanumeric(6);
        try (var conn = cosmos.getDataSource().getConnection()) {
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);

            // normal case
            {
                var records = IntStream.range(0, 202).mapToObj(i -> new PostgresRecord(UUID.randomUUID().toString(), Maps.newHashMap(Map.of("id", i, "name", "John Doe " + i)))).toList();
                var ret = TableUtil.batchInsertRecords(conn, schemaName, tableName, records);
                assertThat(ret.size()).isEqualTo(202);
                assertThat(ret.stream().map(doc -> doc.toMap().getOrDefault("id", "")).toList())
                        .isEqualTo(records.stream().map(r -> r.id).toList());
            }

            // irregular input values
            {
                // records are null
                assertThatThrownBy(() -> TableUtil.batchInsertRecords(conn, schemaName, tableName, null))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("records should not be null");
            }
            {
                // records are empty
                var records = new ArrayList<PostgresRecord>();
                var ret = TableUtil.batchInsertRecords(conn, schemaName, tableName, records);
                assertThat(ret).isEmpty();
            }
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }


    @Test
    void batchDeleteRecords_should_work() throws Exception {
        var tableName = "batchDeleteRecords_test_" + RandomStringUtils.randomAlphanumeric(6);
        try (var conn = cosmos.getDataSource().getConnection()) {
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);

            // normal case
            {
                var records = IntStream.range(0, 200).mapToObj(i -> new PostgresRecord(UUID.randomUUID().toString(), Maps.newHashMap(Map.of("id", i)))).toList();
                var inserted = TableUtil.batchInsertRecords(conn, schemaName, tableName, records);
                assertThat(inserted).hasSize(200);

                var deleteRecords = records.subList(3, 7).stream().map(r -> r.id).toList();
                var ret = TableUtil.batchDeleteRecords(conn, schemaName, tableName, deleteRecords);
                assertThat(ret).hasSize(4);
                assertThat(ret.stream().map(m -> m.toMap().getOrDefault("id", "")).toList())
                        .isEqualTo(deleteRecords);

                var record2 = TableUtil.readRecord(conn, schemaName, tableName, records.get(2).id);
                assertThat(record2.id).isEqualTo(records.get(2).id);

                var record8 = TableUtil.readRecord(conn, schemaName, tableName, records.get(8).id);
                assertThat(record8.id).isEqualTo(records.get(8).id);

            }

            // irregular input values
            {
                // records are null
                assertThatThrownBy(() -> TableUtil.batchDeleteRecords(conn, schemaName, tableName, null))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("ids should not be null");
            }
            {
                // records are empty
                var records = new ArrayList<String>();
                var ret = TableUtil.batchDeleteRecords(conn, schemaName, tableName, records);
                assertThat(ret).isEmpty();
            }
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }

    @Test
    void schemaExists_should_work() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            assertThat(TableUtil.schemaExists(conn, schemaName)).isTrue();
            assertThat(TableUtil.schemaExists(conn, "not_exist_schema")).isFalse();
        }
    }


    @Test
    void batchUpsertRecords_should_work() throws Exception {
        var tableName = "batchUpsertRecords_test_" + RandomStringUtils.randomAlphanumeric(6);
        try (var conn = cosmos.getDataSource().getConnection()) {
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);

            // normal case for insert
            {
                var records = IntStream.range(0, 10).mapToObj(i -> new PostgresRecord("upsert" + i, Maps.newHashMap(Map.of("id", i, "name", "John Doe " + i)))).toList();
                var ret = TableUtil.batchUpsertRecords(conn, schemaName, tableName, records);
                assertThat(ret.size()).isEqualTo(10);
                assertThat(ret.stream().map(doc -> doc.toMap().getOrDefault("id", "")).toList())
                        .isEqualTo(records.stream().map(r -> r.id).toList());
            }

            // normal case for half insert, half update
            {
                var records = IntStream.range(0, 10).mapToObj(i -> new PostgresRecord("half_upsert" + i, Maps.newHashMap(Map.of("id", i, "name", "John Insert " + i)))).toList();
                // insert half records
                TableUtil.batchInsertRecords(conn, schemaName, tableName, records.subList(0, 5));

                // replace name from Insert to Upsert, to get ready for upsert.
                var records2 = records.stream().map(record -> new PostgresRecord(record.id, Maps.newHashMap(Map.of("name", record.data.get("name").toString().replace("Insert", "Upsert"))))).toList();
                var start = 2;
                var end = 8;
                var ret = TableUtil.batchUpsertRecords(conn, schemaName, tableName, records2.subList(start, end));
                assertThat(ret.size()).isEqualTo(end - start);

                // for inserted or upserted
                for(var i = 0; i < end; i++) {
                    var record = TableUtil.readRecord(conn, schemaName, tableName, records.get(i).id);
                    if(i < start || i >= end) {
                        // insert only (not upserted)
                        assertThat(record.data.get("name")).isEqualTo("John Insert " + i);
                    } else {
                        // upserted
                        assertThat(record.data.get("name")).isEqualTo("John Upsert " + i);
                    }
                }

                // for not upserted
                for(var i = end; i < 10; i++) {
                    var record = TableUtil.readRecord(conn, schemaName, tableName, records.get(i).id);
                    assertThat(record).isNull();
                }
            }

            // irregular input values
            {
                // records are null
                assertThatThrownBy(() -> TableUtil.batchUpsertRecords(conn, schemaName, tableName, null))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("records should not be null");
            }
            {
                // records are empty
                var records = new ArrayList<PostgresRecord>();
                var ret = TableUtil.batchUpsertRecords(conn, schemaName, tableName, records);
                assertThat(ret).isEmpty();
            }
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }

    @Test
    void findRecords_should_work() throws Exception {

        // short table name
        var tableName1 = "findRecords_test_" + RandomStringUtils.randomAlphanumeric(6);

        // long table name(> 63 chars)
        var tableName2 = "findRecords_test_" + RandomStringUtils.randomAlphanumeric(63);

        for(var tableName : List.of(tableName1, tableName2)) {

            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                var records = IntStream.range(0, 10).mapToObj(i -> new PostgresRecord(String.valueOf(i), Maps.newHashMap(Map.of(
                                "id", String.valueOf(i),
                                "name", "John Doe " + i,
                                "age", i,
                                "address", Map.of("city", "NY" + i)
                        )
                ))).toList();
                TableUtil.batchUpsertRecords(conn, schemaName, tableName, records);

                var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);

                {
                    // normal case

                    // filtering with id
                    var querySpec = new CosmosSqlQuerySpec();
                    querySpec.setQueryText("SELECT * FROM %s.%s WHERE id = @param000_id".formatted(formattedSchemaName, formattedTableName));
                    querySpec.addParameter(new CosmosSqlParameter("@param000_id", "5"));
                    var found = TableUtil.findRecords(conn, schemaName, tableName, querySpec);
                    assertThat(found).hasSize(1);
                    assertThat(found.get(0).id).isEqualTo(records.get(5).id);
                    assertThat(found.get(0).data).isEqualTo(records.get(5).data);
                }

                {
                    // filtering with name and age
                    var querySpec = new CosmosSqlQuerySpec();
                    querySpec.setQueryText("SELECT * FROM %s.%s WHERE (data->>'name' > @param000_name) AND ((data->>'age')::int < @param001_age)".formatted(formattedSchemaName, formattedTableName));
                    querySpec.addParameter(new CosmosSqlParameter("@param000_name", "John Doe 2"));
                    querySpec.addParameter(new CosmosSqlParameter("@param001_age", 6));
                    var found = TableUtil.findRecords(conn, schemaName, tableName, querySpec);
                    assertThat(found).hasSize(3);
                    assertThat(found.get(0).id).isEqualTo(records.get(3).id);
                    assertThat(found.get(1).data).isEqualTo(records.get(4).data);
                    assertThat(found.get(2).id).isEqualTo(records.get(5).id);
                }

                {
                    // filtering address.city (nested fields)
                    var querySpec = new CosmosSqlQuerySpec();
                    querySpec.setQueryText("SELECT * FROM %s.%s WHERE (data->'address'->>'city' = @param000_city)".formatted(formattedSchemaName, formattedTableName));
                    querySpec.addParameter(new CosmosSqlParameter("@param000_city", "NY5"));
                    var found = TableUtil.findRecords(conn, schemaName, tableName, querySpec);
                    assertThat(found).hasSize(1);
                    assertThat(found.get(0).id).isEqualTo(records.get(5).id);
                    assertThat(found.get(0).data).isEqualTo(records.get(5).data);
                }

                {
                    // irregular input values
                    // filter is null
                    assertThatThrownBy(() -> TableUtil.findRecords(conn, schemaName, tableName, null))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("querySpec should not be null");
                }
                {
                    // filter is empty
                    assertThatThrownBy(() -> TableUtil.findRecords(conn, schemaName, tableName, new CosmosSqlQuerySpec()))
                            .isInstanceOf(IllegalArgumentException.class)
                            .hasMessage("querySpec.queryText should be non-blank");
                }
            } finally {
                try (var conn = cosmos.getDataSource().getConnection()) {
                    TableUtil.dropTableIfExists(conn, schemaName, tableName);
                }
            }
        }
    }

    @Test
    void countRecords_should_work() throws Exception {
        var tableName = "countrecords_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);
        var records = IntStream.range(0, 10).mapToObj(i -> new PostgresRecord(String.valueOf(i), Maps.newHashMap(Map.of("name", "John Doe " + i, "age", i)))).toList();
        try (var conn = cosmos.getDataSource().getConnection()) {
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);
            var ret = TableUtil.batchInsertRecords(conn, schemaName, tableName, records);
            assertThat(ret.size()).isEqualTo(10);

            {
                // normal case
                var querySpec = new CosmosSqlQuerySpec();
                querySpec.setQueryText("SELECT COUNT(*) FROM %s.%s WHERE data->>'name' = @param000_name".formatted(formattedSchemaName, formattedTableName));
                querySpec.addParameter(new CosmosSqlParameter("@param000_name", "John Doe 2"));
                var count = TableUtil.countRecords(conn, schemaName, tableName, querySpec);
                assertThat(count).isEqualTo(1);
            }
            {
                // irregular input values
                // filter is null
                assertThatThrownBy(() -> TableUtil.countRecords(conn, schemaName, tableName, null))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("querySpec should not be null");

                // querySpec.queryText is empty
                var querySpec = new CosmosSqlQuerySpec();
                assertThatThrownBy(() -> TableUtil.countRecords(conn, schemaName, tableName, querySpec))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("querySpec.queryText should be non-blank");
            }


        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }


    @Test
    void getIndexName_should_work() {
        {
            // normal cases
            assertThat(TableUtil.getIndexName("table_name", "column_name")).isEqualTo("idx_table_name_column_name_1");
            assertThat(TableUtil.getIndexName("table1", "_uniqueKey1")).isEqualTo("\"idx_table1__uniqueKey1_1\"");
            assertThat(TableUtil.getIndexName("table1", "address.city.street")).isEqualTo("idx_table1_address_city_street_1");

            // long index names
            assertThat(TableUtil.getIndexName("table1", "contents.77993598-a9d2-4862-ae77-73005d274697.v--alue")).isEqualTo("idx_table1_contents_77993598_a9d_34a61b2575d6dae1___alue_1").hasSizeLessThan(64);
            assertThat(TableUtil.getIndexName("mastersheets_standardremunerationmonthlyamountgrademaster", "_uniqueKey1")).isEqualTo("\"idx_mastersheets_standardremuner_c602b16a8373b1aa_ueKey1_1\"").hasSizeLessThan(64);
            assertThat(TableUtil.getIndexName("mastersheets_standardremunerationmonthlyamountgrademaster_recycle", "_uniqueKey1")).isEqualTo("\"idx_mastersheets_standardremuner_e93ec18d49c213d8_ueKey1_1\"").hasSizeLessThan(64);




        }

        {
            // irregular cases
            assertThatThrownBy(() -> TableUtil.getIndexName("table_name", ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("entityName should be non-blank");
            assertThatThrownBy(() -> TableUtil.getIndexName("", "column_name"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("entityName should be non-blank");
            assertThatThrownBy(() -> TableUtil.getIndexName("table_name", ";"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("entityName should not contain invalid characters: ;");
        }
    }


    @Test
    void createIndexIfNotExists_should_work() throws Exception {

        var tableName = "table1" + RandomStringUtils.randomAlphanumeric(3).toLowerCase();
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);
        var fieldName = "address.city.street";
        var formattedIndexName = TableUtil.getIndexName(formattedTableName, fieldName);

        try(var conn = cosmos.getDataSource().getConnection();) {
            {
                // create table
                var createdTableName = TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                assertThat(createdTableName).isEqualTo(formattedSchemaName + "." + formattedTableName);
            }

            {
                // create index
                var createdIndexName = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(true));
                assertThat(createdIndexName).isEqualTo(formattedSchemaName + "." + formattedIndexName);
            }

            {
                // create index the second time. and it should be no-op
                var created2 = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(true));
                assertThat(created2).isEmpty();
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropIndexIfExists(conn, schemaName, formattedIndexName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }

    @Test
    void createIndexIfNotExists_should_work_for_unique() throws Exception {

        var tableName = "table1" + RandomStringUtils.randomAlphanumeric(3).toLowerCase();
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);
        var fieldName = "_uniqueKey1";
        var formattedIndexName = TableUtil.getIndexName(formattedTableName, fieldName);

        try(var conn = cosmos.getDataSource().getConnection();) {
            {
                // create table
                var createdTableName = TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                assertThat(createdTableName).isEqualTo(formattedSchemaName + "." + formattedTableName);
            }

            {
                // create index
                var createdIndexName = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(true));
                assertThat(createdIndexName).isEqualTo(formattedSchemaName + "." + formattedIndexName);
            }

            {
                // create index the second time. and it should be no-op
                var created2 = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(true));
                assertThat(created2).isEmpty();
            }

            {
                // test unique constraint
                var id = "uniqueKey1_test_" + RandomStringUtils.randomAlphanumeric(6);
                TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id, Maps.newLinkedHashMap(Map.of(fieldName, "1"))));

                var id2 = "uniqueKey1_test_" + RandomStringUtils.randomAlphanumeric(6);
                // although the id is same, but the fieldName is duplicated. so it should fail
                assertThatThrownBy(() -> TableUtil.upsertRecord(conn, schemaName, tableName, new PostgresRecord(id2, Maps.newLinkedHashMap(Map.of(fieldName, "1")))))
                        .isInstanceOfSatisfying(SQLException.class, e -> {
                            assertThat(e.getSQLState()).isEqualTo("23505");
                            assertThat(e.getMessage()).contains("duplicate key");
                        });
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropIndexIfExists(conn, schemaName, formattedIndexName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }

    @Test
    void createIndexIfNotExists_should_work_for_expireAt() throws Exception {

        var tableName = "table1" + RandomStringUtils.randomAlphanumeric(3).toLowerCase();
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);
        var fieldName = "_expireAt";
        var formattedIndexName = TableUtil.getIndexName(formattedTableName, fieldName);

        try(var conn = cosmos.getDataSource().getConnection();) {
            {
                // create table
                var createdTableName = TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                assertThat(createdTableName).isEqualTo(formattedSchemaName + "." + formattedTableName);
            }

            {
                // create index
                var createdIndexName = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName,
                        IndexOption.unique(false).fieldType("bigint"));
                assertThat(createdIndexName).isEqualTo(formattedSchemaName + "." + formattedIndexName);


                var indexNameWithoutQuotes = TableUtil.removeQuotes(formattedIndexName);
                var queryIndexSQL = "SELECT indexdef FROM pg_indexes WHERE indexname = '%s'".formatted(indexNameWithoutQuotes);

                try(var stmt = conn.createStatement();
                     var rs = stmt.executeQuery(queryIndexSQL)) {

                    // Check that the index exists.
                    assertThat(rs.next()).isTrue();

                    // Verify the index definition includes the bigint cast.
                    var indexDefinition = rs.getString("indexdef");
                    assertThat(indexDefinition).isNotNull();
                    // postgres automatically add the ::text part, so finally the index looks like below:
                    assertThat(indexDefinition).contains("((data ->> '_expireAt'::text))::bigint")
                            .containsIgnoringCase("USING BTREE");

                }
            }

            {
                // create index the second time. and it should be no-op
                var created2 = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(false));
                assertThat(created2).isEmpty();
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropIndexIfExists(conn, schemaName, formattedIndexName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }


    @Test
    void getShortenedEntityName_should_work() {
        {
            // normal case
            assertThat(TableUtil.getShortenedEntityName("table_name")).isEqualTo("table_name");
            assertThat(TableUtil.getShortenedEntityName("mastersheets_standardremunerationmonthlyamountgrademaster"))
                    .isEqualTo("mastersheets_standardremunerationmonthlyamountgrademaster");
        }

        {
            // length > 63
            var longName = "a" + IntStream.range(0, 63).mapToObj(i -> "x").collect(Collectors.joining());
            var expected = "axxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx_3486051ba0508e5c_xxxxxxxx";
            var shortenedName = TableUtil.getShortenedEntityName(longName);
            assertThat(shortenedName).isEqualTo(expected).hasSize(58);
        }

        {
            // length > 63, table name in real world
            var longName = "mastersheets_standardremunerationmonthlyamountgrademaster_recycle";
            var expected = "mastersheets_standardremuneratio_89d2e9f3e7423b7c__recycle";
            var shortenedName = TableUtil.getShortenedEntityName(longName);
            assertThat(shortenedName).isEqualTo(expected).hasSize(58);
        }

        {
            // irregular cases
            assertThat(TableUtil.getShortenedEntityName(null)).isEqualTo(null);
            assertThat(TableUtil.getShortenedEntityName("")).isEqualTo("");
        }
    }

    private static final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern("java-cosmos"+"-%s").build();
    private static final ThreadPoolExecutor SINGLE_TASK_EXECUTOR = (ThreadPoolExecutor) Executors.newFixedThreadPool(100, threadFactory);


    @Test
    void createTableIfNotExist_should_work_in_multi_thread() throws Exception {

        var tableName = "table1_multi_thread_test";
        try {
            List<Future<String>> futures = new ArrayList<>();

            int threadCount = 100;

            for (int i = 0; i < threadCount; i++) {
                futures.add(SINGLE_TASK_EXECUTOR.submit(() -> {

                    try (var conn = cosmos.getDataSource().getConnection()) {
                        return TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }

                }));
            }

            List<String> results = new ArrayList<>();

            for (int i = 0; i < threadCount; i++) {
                results.add(futures.get(i).get());
            }

            assertThat(results).hasSize(threadCount);

            results = results.stream().filter(StringUtils::isNotEmpty).toList();

            // only few threads execute "CREATE TABLE IF NOT EXISTS"
            assertThat(results).hasSizeLessThan(5);
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }

}