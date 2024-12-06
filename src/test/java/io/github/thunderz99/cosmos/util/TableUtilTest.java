package io.github.thunderz99.cosmos.util;

import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

        try {

            {
                var conn = cosmos.getDataSource().getConnection();
                //table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);
                assertThat(tableExist).isFalse();
            }
            {
                var conn = cosmos.getDataSource().getConnection();
                //create table
                TableUtil.createTableIfNotExists(conn, schemaName, tableName);

                //table exists
                var conn2 = cosmos.getDataSource().getConnection();
                var tableExist = TableUtil.tableExist(conn2, schemaName, tableName);
                assertThat(tableExist).isTrue();
            }

            {
                var conn = cosmos.getDataSource().getConnection();
                //drop table
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
                //table does not exist
                var tableExist = TableUtil.tableExist(conn, schemaName, tableName);

                assertThat(tableExist).isFalse();
            }

        } finally {
            var conn = cosmos.getDataSource().getConnection();
            TableUtil.dropTableIfExists(conn, schemaName, tableName);
        }

    }

}