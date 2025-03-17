package io.github.thunderz99.cosmos.impl.postgres.dto;
/**
 * DTO class representing a job in PG-Cron.
 */
public class PGJob {

    /**
     * The id of the job.
     */
    public String id;

    /**
     * The name of the job.
     */
    public String jobName;

    /**
     * The schedule of the job.
     */
    public String schedule;

    /**
     * The SQL command the job will execute.
     */
    public String command;
}
