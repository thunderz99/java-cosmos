package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImplTest;
import io.github.thunderz99.cosmos.util.EnvUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PGCronUtilTest {

    static PostgresImpl cosmos;

    static final String dbName = "java_cosmos";
    static final String schemaName = "cron_util_test_" + StringUtils.lowerCase(RandomStringUtils.randomAlphanumeric(4));

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
    void scheduleCustomJob_should_work() throws Exception {
        var jobName = "test_custom_job_" + RandomStringUtils.randomAlphanumeric(4);
        var cronExpression = "*/5 * * * *";
        var sql = "SELECT 1;";

        try (var conn = cosmos.getDataSource().getConnection()) {
            // schedule custom job
            var jobId = PGCronUtil.scheduleCustomJob(conn, jobName, cronExpression, sql);
            assertThat(jobId).isGreaterThan(0);

            // check whether the job exists
            assertThat(PGCronUtil.jobExists(conn, jobName)).isTrue();

            // check that the job details are correct
            var job = PGCronUtil.findJobByName(conn, jobName);
            assertThat(job).isNotNull();
            assertThat(job.jobName).isEqualTo(jobName);
            assertThat(job.schedule.trim()).isEqualTo(cronExpression);
            assertThat(job.command.trim()).isEqualTo(sql);

            // un-schedule job
            var unScheduled = PGCronUtil.unScheduleJob(conn, jobName);
            assertThat(unScheduled).isTrue();

            // check whether the job exists
            assertThat(PGCronUtil.jobExists(conn, jobName)).isFalse();
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                PGCronUtil.unScheduleJob(conn, jobName);
            }
        }
    }

    @Test
    void scheduleCustomJob_should_work_with_complex_sql() throws Exception {
        var jobName = "test_complex_job_" + RandomStringUtils.randomAlphanumeric(4);
        var tableName = "test_table_" + RandomStringUtils.randomAlphanumeric(4);
        var cronExpression = "0 0 * * *";
        var sql = String.format("""
                DELETE FROM %s.%s
                WHERE (data->>'_expireAt')::bigint < extract(epoch from now())::bigint;
                """, schemaName, tableName);

        try (var conn = cosmos.getDataSource().getConnection()) {
            // create table first
            TableUtil.createTableIfNotExists(conn, schemaName, tableName);

            // schedule custom job
            var jobId = PGCronUtil.scheduleCustomJob(conn, jobName, cronExpression, sql);
            assertThat(jobId).isGreaterThan(0);

            // check whether the job exists
            assertThat(PGCronUtil.jobExists(conn, jobName)).isTrue();

            // check that the job details are correct
            var job = PGCronUtil.findJobByName(conn, jobName);
            assertThat(job).isNotNull();
            assertThat(job.jobName).isEqualTo(jobName);
            assertThat(job.schedule.trim()).isEqualTo(cronExpression);
            assertThat(job.command).contains(schemaName);
            assertThat(job.command).contains(tableName);

            // un-schedule job
            var unScheduled = PGCronUtil.unScheduleJob(conn, jobName);
            assertThat(unScheduled).isTrue();

            // check whether the job exists
            assertThat(PGCronUtil.jobExists(conn, jobName)).isFalse();
        } finally {
            try (var conn = cosmos.getDataSource().getConnection()) {
                PGCronUtil.unScheduleJob(conn, jobName);
                TableUtil.dropTableIfExists(conn, schemaName, tableName);
            }
        }
    }

    @Test
    void scheduleCustomJob_should_throw_for_blank_jobName() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            assertThatThrownBy(() -> PGCronUtil.scheduleCustomJob(conn, "", "*/5 * * * *", "SELECT 1;"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jobName should be non-blank");

            assertThatThrownBy(() -> PGCronUtil.scheduleCustomJob(conn, null, "*/5 * * * *", "SELECT 1;"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jobName should be non-blank");
        }
    }

    @Test
    void scheduleCustomJob_should_throw_for_invalid_cronExpression() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            assertThatThrownBy(() -> PGCronUtil.scheduleCustomJob(conn, "test_job", "invalid_cron", "SELECT 1;"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cronExpression is not valid");
        }
    }

    @Test
    void scheduleCustomJob_should_throw_for_blank_sql() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            assertThatThrownBy(() -> PGCronUtil.scheduleCustomJob(conn, "test_job", "*/5 * * * *", ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sql should be non-blank");

            assertThatThrownBy(() -> PGCronUtil.scheduleCustomJob(conn, "test_job", "*/5 * * * *", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sql should be non-blank");
        }
    }

    @Test
    void unScheduleJob_should_return_false_for_non_existent_job() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            var result = PGCronUtil.unScheduleJob(conn, "non_existent_job_" + RandomStringUtils.randomAlphanumeric(8));
            assertThat(result).isFalse();
        }
    }

    @Test
    void jobExists_should_return_false_for_non_existent_job() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            var exists = PGCronUtil.jobExists(conn, "non_existent_job_" + RandomStringUtils.randomAlphanumeric(8));
            assertThat(exists).isFalse();
        }
    }

    @Test
    void findJobByName_should_return_null_for_non_existent_job() throws Exception {
        try (var conn = cosmos.getDataSource().getConnection()) {
            var job = PGCronUtil.findJobByName(conn, "non_existent_job_" + RandomStringUtils.randomAlphanumeric(8));
            assertThat(job).isNull();
        }
    }
}
