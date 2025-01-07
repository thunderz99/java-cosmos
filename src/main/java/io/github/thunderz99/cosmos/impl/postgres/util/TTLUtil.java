package io.github.thunderz99.cosmos.impl.postgres.util;

import java.sql.Connection;
import java.sql.SQLException;

import io.github.thunderz99.cosmos.util.Checker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for ttl feature using pg_cron plugin. provides schedule / un-schedule cron jobs that delete expired records
 */
public class TTLUtil {
    private static final Logger log = LoggerFactory.getLogger(TTLUtil.class);


    /**
     * schedule a job to delete expired records
     *
     * @param conn              the database connection
     * @param schemaName        the schema name
     * @param tableName         the table name
     * @param intervalInMinutes interval in minutes
     * @return jobId of the scheduled job. return -1 if job is not created.
     * @throws IllegalArgumentException if intervalInMinutes <= 0
     * @throws SQLException if the table does not exist
     * @throws SQLException if a database error occurs
     *
     */
    public static long scheduleJob(Connection conn, String schemaName, String tableName, int intervalInMinutes) throws SQLException {

        Checker.check(intervalInMinutes > 0, "intervalInMinutes must > 0. %s.%s".formatted(schemaName, tableName));

        schemaName = TableUtil.checkAndNormalizeValidEntityName(schemaName);
        tableName = TableUtil.checkAndNormalizeValidEntityName(tableName);


        if(!TableUtil.tableExist(conn, schemaName, tableName)){
            // SQLState 42P01 is for "relation not exist" in postgres
            throw new SQLException(String.format("table %s.%s does not exist / relation does not exist", schemaName, tableName), "42P01", 404);
        }

        /**
         * -- sample SQL
         * SELECT cron.schedule(
         *     'localhost_ttl_job_sessioninfoes',  -- Job name
         *     '*\/1 * * * * ',          -- Run every 1 minutes
         *     $$
         *     DELETE FROM sessioninfoes -- table name
         *     WHERE(data -> > '/_expireAt')::timestamp < now(); -- finding expired records
         *     $$
         *  );
         */

        var jobName = getJobName(schemaName, tableName);

        var deleteSQL = String.format("""
                DELETE FROM %s.%s
                WHERE(%s ->> '/_expireAt')::timestamp < now();
                """, schemaName, tableName, TableUtil.DATA);

        var scheduleSQL = String.format("""
                SELECT cron.schedule(
                    '%s',
                    '*/%d * * * * ',
                     $$
                     %s
                     $$
                );
                """, jobName, intervalInMinutes, intervalInMinutes, deleteSQL);

        // the stmt will return the job id
        try (var stmt = conn.prepareStatement(scheduleSQL)) {
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        return -1L;
    }


    /**
     * un-schedule the job to delete expired records
     *
     * @param conn       the database connection
     * @param schemaName the schema name
     * @param tableName  the table name
     * @return true if the job is un-scheduled, false if the job is not found
     * @throws SQLException if a database error occurs
     */
    public static boolean unScheduleJob(Connection conn, String schemaName, String tableName) throws SQLException {

        schemaName = TableUtil.checkAndNormalizeValidEntityName(schemaName);
        tableName = TableUtil.checkAndNormalizeValidEntityName(tableName);

        // sample SQL: SELECT cron.unschedule(jobName);
        var jobName = getJobName(schemaName, tableName);
        try (var stmt = conn.prepareStatement(String.format("SELECT cron.unschedule('%s');", jobName))) {
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getBoolean(1);
                }
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("could not find valid entry for job")) {
                return false;
            }
            throw e;
        }

        return false;
    }


    /**
     * check whether the job exists
     *
     * @param conn       the database connection
     * @param schemaName the schema name
     * @param tableName  the table name
     * @return true if the job exists, false if the job not exists
     * @throws SQLException if a database error occurs
     */
    public static boolean jobExists(Connection conn, String schemaName, String tableName) throws SQLException {

        schemaName = TableUtil.checkAndNormalizeValidEntityName(schemaName);
        tableName = TableUtil.checkAndNormalizeValidEntityName(tableName);

        var jobName = getJobName(schemaName, tableName);

        /**
         *  SELECT * FROM cron.job WHERE jobname = ?
         */
        try (var stmt = conn.prepareStatement("SELECT * FROM cron.job WHERE jobname = ?")) {
            stmt.setString(1, jobName);
            try (var rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * get the jobName for schemaName and tableName, which deletes records according to TTL
     *
     * @param schemaName the schema name
     * @param tableName  the table name
     * @return the jobName
     */
    public static String getJobName(String schemaName, String tableName) {
        Checker.checkNotBlank(schemaName, "schemaName");
        Checker.checkNotBlank(tableName, "tableName");
        return String.format("%s_ttl_job_%s", schemaName, tableName).toLowerCase();
    }

}
