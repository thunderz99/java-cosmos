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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableUtilTest {

    static PostgresImpl cosmos;

    static final String dbName = "java_cosmos";
    static final String schemaName = "table_util_test_schema_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();

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


        var tableName = "create_table_if_not_exist_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();

        try (var conn = cosmos.getDataSource().getConnection()) {
            {

                // table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);
                assertThat(tableExist).isFalse();
            }
            {
                // create table
                var created = TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                assertThat(created).isEqualTo( schemaName + "." + tableName);

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

        var tableName = "crud_test_" + RandomStringUtils.randomAlphanumeric(6);

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

        var tenantId = "Data_upsert_" + RandomStringUtils.randomAlphanumeric(6);

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

        var tableName = "upsert_partial_test_" + RandomStringUtils.randomAlphanumeric(6).toLowerCase();

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
                                assertThat(e.getMessage()).contains("table:'%s.%s', id:%s".formatted(schemaName, tableName, id));
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
        var tableName = "findRecords_test_" + RandomStringUtils.randomAlphanumeric(6);

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

            {
                // normal case

                // filtering with id
                var querySpec = new CosmosSqlQuerySpec();
                querySpec.setQueryText("SELECT * FROM %s.%s WHERE id = @param000_id".formatted(schemaName, tableName));
                querySpec.addParameter(new CosmosSqlParameter("@param000_id", "5"));
                var found = TableUtil.findRecords(conn, schemaName, tableName, querySpec);
                assertThat(found).hasSize(1);
                assertThat(found.get(0).id).isEqualTo(records.get(5).id);
                assertThat(found.get(0).data).isEqualTo(records.get(5).data);
            }

            {
                // filtering with name and age
                var querySpec = new CosmosSqlQuerySpec();
                querySpec.setQueryText("SELECT * FROM %s.%s WHERE (data->>'name' > @param000_name) AND ((data->>'age')::int < @param001_age)".formatted(schemaName, tableName));
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
                querySpec.setQueryText("SELECT * FROM %s.%s WHERE (data->'address'->>'city' = @param000_city)".formatted(schemaName, tableName));
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

    @Test
    void countRecords_should_work() throws Exception {
        var tableName = "countrecords_" + RandomStringUtils.randomAlphanumeric(4).toLowerCase();
        var records = IntStream.range(0, 10).mapToObj(i -> new PostgresRecord(String.valueOf(i), Maps.newHashMap(Map.of("name", "John Doe " + i, "age", i)))).toList();
        try (var conn = cosmos.getDataSource().getConnection()) {
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);
            var ret = TableUtil.batchInsertRecords(conn, schemaName, tableName, records);
            assertThat(ret.size()).isEqualTo(10);

            {
                // normal case
                var querySpec = new CosmosSqlQuerySpec();
                querySpec.setQueryText("SELECT COUNT(*) FROM %s.%s WHERE data->>'name' = @param000_name".formatted(schemaName, tableName));
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
            assertThat(TableUtil.getIndexName("table_name", "column_name")).isEqualTo("table_name_column_name_1");
            assertThat(TableUtil.getIndexName("table1", "_uniqueKey1")).isEqualTo("table1__uniquekey1_1");
            assertThat(TableUtil.getIndexName("table1", "address.city.street")).isEqualTo("table1_address_city_street_1");
            assertThat(TableUtil.getIndexName("table1", "contents.77993598-a9d2-4862-ae77-73005d274697.v--alue")).isEqualTo("table1_contents_77993598_a9d2_4862_ae77_73005d274697_v__alue_1").hasSizeLessThan(64);

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
        var fieldName = "address.city.street";
        var indexName = tableName + "_address_city_street_1";

        try(var conn = cosmos.getDataSource().getConnection();) {
            {
                // create table
                var createdTableName = TableUtil.createTableIfNotExists(conn, schemaName, tableName);
                assertThat(createdTableName).isEqualTo(schemaName + "." + tableName);
            }

            {
                // create index
                var createdIndexName = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(true));
                assertThat(createdIndexName).isEqualTo(schemaName + "." + indexName);
            }

            {
                // create index the second time. and it should be no-op
                var created2 = TableUtil.createIndexIfNotExists(conn, schemaName, tableName, fieldName, IndexOption.unique(true));
                assertThat(created2).isEmpty();
            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TableUtil.dropIndexIfExists(conn, schemaName, indexName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }

    }

}