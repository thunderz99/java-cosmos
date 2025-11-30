package io.github.thunderz99.cosmos.impl.postgres.util;

import java.sql.Connection;
import java.sql.SQLException;

import io.github.thunderz99.cosmos.impl.postgres.dto.PGJob;
import io.github.thunderz99.cosmos.util.Checker;
import io.github.thunderz99.cosmos.util.CronUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for pg_cron plugin. Provides schedule / un-schedule cron jobs that accept custom cron expressions and custom SQL scripts.
 * This class is similar to TTLUtil but provides more flexibility for custom SQL scripts.
 */
public class PGCronUtil {
    private static final Logger log = LoggerFactory.getLogger(PGCronUtil.class);

    /**
     * Schedule a custom cron job with a custom SQL script.
     *
     * @param conn           the database connection
     * @param jobName        the unique name of the job
     * @param cronExpression the cron expression (e.g., "*&#47;5 * * * *" for every 5 minutes)
     * @param sql            the SQL script to execute
     * @return jobId of the scheduled job. return -1 if job is not created.
     * @throws IllegalArgumentException if jobName is blank, cronExpression is invalid, or sql is blank
     * @throws SQLException             if a database error occurs
     */
    public static long scheduleCustomJob(Connection conn, String jobName, String cronExpression, String sql) throws SQLException {

        Checker.checkNotBlank(jobName, "jobName");
        Checker.checkNotBlank(sql, "sql");
        Checker.check(CronUtil.isValidPgCronExpression(cronExpression), "cronExpression is not valid: %s".formatted(cronExpression));

        try (var stmt = conn.prepareStatement("SELECT cron.schedule(?, ?, ?);")) {
            stmt.setString(1, jobName);
            stmt.setString(2, cronExpression);
            stmt.setString(3, sql);
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    var jobId = rs.getLong(1);
                    if (log.isInfoEnabled()) {
                        log.info("scheduleCustomJob '{}' successfully. cronExpression: {}", jobName, cronExpression);
                    }
                    return jobId;
                }
            }
        }
        return -1L;
    }

    /**
     * Un-schedule a cron job by its name.
     *
     * @param conn    the database connection
     * @param jobName the name of the job to un-schedule
     * @return true if the job is un-scheduled, false if the job is not found
     * @throws SQLException if a database error occurs
     */
    public static boolean unScheduleJob(Connection conn, String jobName) throws SQLException {

        Checker.checkNotBlank(jobName, "jobName");

        try (var stmt = conn.prepareStatement("SELECT cron.unschedule(?);")) {
            stmt.setString(1, jobName);
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    if (log.isInfoEnabled()) {
                        log.info("unScheduleJob '{}' successfully.", jobName);
                    }
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
     * Check whether a job exists by its name.
     *
     * @param conn    the database connection
     * @param jobName the name of the job
     * @return true if the job exists, false if the job does not exist
     * @throws SQLException if a database error occurs
     */
    public static boolean jobExists(Connection conn, String jobName) throws SQLException {

        Checker.checkNotBlank(jobName, "jobName");

        try (var stmt = conn.prepareStatement("SELECT * FROM cron.job WHERE jobname = ?")) {
            stmt.setString(1, jobName);
            try (var rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    /**
     * Find a job by its name and return its details.
     *
     * @param conn    the database connection
     * @param jobName the name of the job
     * @return PGJob object containing job details, or null if not found
     * @throws SQLException if a database error occurs
     */
    public static PGJob findJobByName(Connection conn, String jobName) throws SQLException {

        Checker.checkNotBlank(jobName, "jobName");

        var ret = new PGJob();
        try (var stmt = conn.prepareStatement("SELECT jobid, jobname, schedule, command FROM cron.job WHERE jobname = ?")) {
            stmt.setString(1, jobName);
            try (var rs = stmt.executeQuery()) {
                if (rs.next()) {
                    ret.id = rs.getString("jobid");
                    ret.jobName = rs.getString("jobname");
                    ret.schedule = rs.getString("schedule");
                    ret.command = rs.getString("command");
                    return ret;
                }
            }
        }
        return null;
    }
}
