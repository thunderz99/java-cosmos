package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImplTest;
import io.github.thunderz99.cosmos.util.EnvUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TTLUtilTest {

    static PostgresImpl cosmos;

    /**
     * does not have effect. the db specified in the connection string will be used.
     */
    static final String dbName = "java_cosmos";
    static final String schemaName = "ttl_util_test_schema_" + StringUtils.lowerCase(RandomStringUtils.randomAlphanumeric(4));
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
    void scheduleJob_should_work() throws Exception {
        var tableName = "schedule_job_test_" + RandomStringUtils.randomAlphanumeric(4);
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);


        try (var conn = cosmos.getDataSource().getConnection()) {
            // create table
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);

            { // normal case
                // schedule job
                var jobId = TTLUtil.scheduleJob(conn, schemaName, tableName, 1);
                assertThat(jobId).isGreaterThan(0);

                // check whether the job exists
                assertThat(TTLUtil.jobExists(conn, schemaName, tableName)).isTrue();

                // check that the command(text) is correct
                var expectedCmd = """                        
                        DELETE FROM %s.%s
                        WHERE (data->>'_expireAt')::bigint < extract(epoch from now())::bigint;
                        """.trim().formatted(schemaName, formattedTableName);
                var job = TTLUtil.findJobByName(conn, schemaName, tableName);
                assertThat(job.command).contains(schemaName).contains(tableName);
                assertThat(job.command.trim()).isEqualTo(expectedCmd);
                assertThat(job.schedule.trim()).isEqualTo("*/1 * * * *");

                // un-schedule job
                var unScheduled = TTLUtil.unScheduleJob(conn, schemaName, tableName);
                assertThat(unScheduled).isTrue();

                // check whether the job exists
                assertThat(TTLUtil.jobExists(conn, schemaName, tableName)).isFalse();
            }

            {
                // irregular case:  table not exist
                // schedule job
                assertThatThrownBy( () -> TTLUtil.scheduleJob(conn, schemaName, "not_exist_table", 1))
                        .isInstanceOfSatisfying(SQLException.class, e -> {
                                    assertThat(e.getMessage()).contains("relation does not exist");
                                    assertThat(e.getMessage()).contains("%s.%s".formatted(formattedSchemaName, "not_exist_table"));
                                    assertThat(e.getSQLState()).isEqualTo("42P01");
                                });

            }

            {
                // irregular case:  schema not exist
                // schedule job
                assertThatThrownBy( () -> TTLUtil.scheduleJob(conn, "not_exist_schema", tableName, 1))
                        .isInstanceOfSatisfying(SQLException.class, e -> {
                            assertThat(e.getMessage()).contains("relation does not exist");
                            assertThat(e.getMessage()).contains("%s.%s".formatted("not_exist_schema", formattedTableName));
                            assertThat(e.getSQLState()).isEqualTo("42P01");
                        });

            }

            {
                // irregular case:  interval <= 0
                // schedule job
                assertThatThrownBy( () -> TTLUtil.scheduleJob(conn, schemaName, tableName, 0))
                        .isInstanceOfSatisfying(IllegalArgumentException.class, e -> {
                            assertThat(e.getMessage()).contains("intervalInMinutes must > 0");
                            assertThat(e.getMessage()).contains("%s.%s".formatted(schemaName, tableName));
                        });

            }

        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TTLUtil.unScheduleJob(conn, schemaName, tableName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }

    @Test
    void scheduleJob_should_work_using_cron_expression() throws Exception {
        var tableName = "schedule_job_exp_test" + RandomStringUtils.randomAlphanumeric(4);
        var formattedTableName = TableUtil.checkAndNormalizeValidEntityName(tableName);


        try (var conn = cosmos.getDataSource().getConnection()) {
            // create table
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);

            { // normal case
                // schedule job
                var jobId = TTLUtil.scheduleJob(conn, schemaName, tableName, "5 0 * * *");
                assertThat(jobId).isGreaterThan(0);

                // check whether the job exists
                assertThat(TTLUtil.jobExists(conn, schemaName, tableName)).isTrue();

                // check that the command(text) is correct
                var expectedCmd = """                        
                        DELETE FROM %s.%s
                        WHERE (data->>'_expireAt')::bigint < extract(epoch from now())::bigint;
                        """.trim().formatted(schemaName, formattedTableName);
                var job = TTLUtil.findJobByName(conn, schemaName, tableName);
                assertThat(job.command).contains(schemaName).contains(tableName);
                assertThat(job.command.trim()).isEqualTo(expectedCmd);
                assertThat(job.schedule.trim()).isEqualTo("5 0 * * *");

                // un-schedule job
                var unScheduled = TTLUtil.unScheduleJob(conn, schemaName, tableName);
                assertThat(unScheduled).isTrue();

                // check whether the job exists
                assertThat(TTLUtil.jobExists(conn, schemaName, tableName)).isFalse();
            }
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                TTLUtil.unScheduleJob(conn, schemaName, tableName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }

    @Test
    void getJobName_should_work() throws Exception {
        assertThat(TTLUtil.getJobName("Schema", "Tables")).isEqualTo("Schema_ttl_job_Tables");
        assertThat(TTLUtil.getJobName("Data_Tom", "Users")).isEqualTo("Data_Tom_ttl_job_Users");
    }

}