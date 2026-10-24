package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImplTest;
import io.github.thunderz99.cosmos.impl.postgres.dto.IndexOption;
import io.github.thunderz99.cosmos.impl.postgres.dto.PGFieldType;
import io.github.thunderz99.cosmos.impl.postgres.dto.PGIndexField;
import io.github.thunderz99.cosmos.util.EnvUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableUtilConcurrentIndexTest {
    private static PostgresImpl cosmos;
    private static final String SCHEMA = "concurrent_index_" + UUID.randomUUID().toString().substring(0, 8);
    private Connection conn;
    private String table;
    private String qualifiedTable;

    @BeforeAll
    static void beforeAll() throws Exception {
        cosmos = new PostgresImpl(EnvUtil.getOrDefault("POSTGRES_CONNECTION_STRING", PostgresImplTest.LOCAL_CONNECTION_STRING));
        cosmos.createIfNotExist("java_cosmos", SCHEMA);
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (cosmos != null) {
            try {
                cosmos.deleteCollection("java_cosmos", SCHEMA);
            } finally {
                cosmos.closeClient();
            }
        }
    }

    @BeforeEach
    void beforeEach() throws Exception {
        conn = cosmos.getDataSource().getConnection();
        table = "items_" + UUID.randomUUID().toString().substring(0, 8);
        qualifiedTable = quoted(SCHEMA) + "." + quoted(table);
        TableUtil.createTableIfNotExists(conn, SCHEMA, table);
        execute("SET lock_timeout = '7s'");
    }

    @AfterEach
    void afterEach() throws Exception {
        if (conn != null) {
            try (var connection = conn) {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
                execute("RESET lock_timeout");
                TableUtil.dropTableIfExists(conn, SCHEMA, table);
            }
        }
    }

    @Test
    void concurrent_single_composite_unique_and_gin_indexes_keep_their_definitions() throws Exception {
        var age = PGIndexField.of("profile.age", PGFieldType.INTEGER);
        assertThat(TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, age, concurrent()))
                .isEqualTo(quoted(SCHEMA) + "." + index("profile.age"));
        assertThat(definition("profile.age")).contains("USING btree", "profile", "age", "integer");
        assertThat(TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, age, concurrent())).isEmpty();

        var fields = List.of(new PGIndexField("email"), PGIndexField.of("number", PGFieldType.BIGINT));
        TableUtil.createIndexIfNotExist4MultiFields(conn, SCHEMA, table, fields, IndexOption.unique(true).concurrently(true));
        assertThat(definition("email_number")).contains("CREATE UNIQUE INDEX", "USING btree", "email", "bigint");

        TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, new PGIndexField("tags"), concurrent().gin());
        assertThat(definition("tags")).contains("USING gin", "data -> 'tags'");
        assertCleanConnection();
    }

    @Test
    @SuppressWarnings("removal")
    void deprecated_entry_point_also_supports_concurrent_creation() throws Exception {
        TableUtil.createIndexIfNotExists(conn, SCHEMA, table, "legacy", concurrent().fieldType("bigint"));
        assertThat(definition("legacy")).contains("bigint");
        assertThat(TableUtil.createIndexIfNotExists(conn, SCHEMA, table, "legacy", concurrent())).isEmpty();
        assertCleanConnection();
    }

    @Test
    void caller_transaction_is_rejected_without_commit_or_rollback_even_when_index_exists() throws Exception {
        TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, new PGIndexField("email"), concurrent());
        conn.setAutoCommit(false);
        execute("INSERT INTO " + qualifiedTable + " (id, data) VALUES ('pending', '{}')");

        assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4SingleField(
                conn, SCHEMA, table, new PGIndexField("email"), concurrent()))
                .isInstanceOf(SQLException.class).hasMessageContaining("autoCommit=true")
                .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("25001");
        assertThat(conn.getAutoCommit()).isFalse();
        assertThat(scalar("SELECT count(*) FROM " + qualifiedTable)).isEqualTo("1");
        conn.rollback();
        conn.setAutoCommit(true);
        assertThat(scalar("SELECT count(*) FROM " + qualifiedTable)).isEqualTo("0");
        assertCleanConnection();
    }

    @Test
    void ordinary_existing_index_does_not_commit_the_callers_pending_transaction() throws Exception {
        TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, new PGIndexField("email"), new IndexOption());
        conn.setAutoCommit(false);
        execute("INSERT INTO " + qualifiedTable + " (id, data) VALUES ('pending', '{}')");
        assertThat(TableUtil.createIndexIfNotExist4SingleField(
                conn, SCHEMA, table, new PGIndexField("email"), new IndexOption().concurrently(false))).isEmpty();
        assertThat(conn.getAutoCommit()).isFalse();
        assertThat(scalar("SELECT count(*) FROM " + qualifiedTable)).isEqualTo("1");
        conn.rollback();
        conn.setAutoCommit(true);
        assertThat(scalar("SELECT count(*) FROM " + qualifiedTable)).isEqualTo("0");
        assertCleanConnection();
    }

    @Test
    void concurrent_build_allows_new_writes_while_waiting_for_an_existing_writer() throws Exception {
        // A normal CREATE INDEX would wait for this writer's RowExclusiveLock.
        try (var writer = cosmos.getDataSource().getConnection()) {
            writer.setAutoCommit(false);
            try (var stmt = writer.createStatement()) {
                stmt.execute("INSERT INTO " + qualifiedTable + " (id, data) VALUES ('pending', '{}')");
            }
            try {
                // PostgreSQL still waits for writers during a concurrent build. Observe its
                // ShareUpdateExclusiveLock, then prove that a new writer is not blocked.
                var executor = Executors.newSingleThreadExecutor();
                try {
                    var created = executor.submit(() -> TableUtil.createIndexIfNotExist4SingleField(
                            conn, SCHEMA, table, new PGIndexField("online"), concurrent(), 5_000));
                    try (var observer = cosmos.getDataSource().getConnection(); var stmt = observer.createStatement()) {
                        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        boolean building = false;
                        while (System.nanoTime() < deadline) {
                            try (var rs = stmt.executeQuery("SELECT 1 FROM pg_locks WHERE relation = '" + qualifiedTable
                                    + "'::regclass AND mode = 'ShareUpdateExclusiveLock' AND granted")) {
                                if (rs.next()) {
                                    building = true;
                                    break;
                                }
                            }
                            Thread.sleep(10);
                        }
                        assertThat(building).isTrue();
                        stmt.execute("SET lock_timeout = '500ms'");
                        try {
                            stmt.execute("INSERT INTO " + qualifiedTable + " (id, data) VALUES ('online', '{}')");
                        } finally {
                            stmt.execute("RESET lock_timeout");
                        }
                    }
                    writer.rollback();
                    assertThat(created.get(10, TimeUnit.SECONDS)).isNotEmpty();
                } finally {
                    writer.rollback();
                    executor.shutdownNow();
                    assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
                }
            } finally {
                writer.rollback();
            }
        }
        assertCleanConnection();
    }

    @Test
    void repeated_creators_share_the_lock_with_ordinary_index_creation() throws Exception {
        assertRepeatedCreators(true);
    }

    @Test
    void repeated_concurrent_creators_create_exactly_one_index() throws Exception {
        assertRepeatedCreators(false);
    }

    private void assertRepeatedCreators(boolean includeOrdinary) throws Exception {
        var executor = Executors.newFixedThreadPool(6);
        var start = new CountDownLatch(1);
        var futures = new ArrayList<Future<String>>();
        try {
            for (int i = 0; i < 6; i++) {
                boolean concurrently = !includeOrdinary || i % 2 == 0;
                futures.add(executor.submit(() -> {
                    try (var worker = cosmos.getDataSource().getConnection()) {
                        start.await();
                        var result = TableUtil.createIndexIfNotExist4SingleField(worker, SCHEMA, table,
                                new PGIndexField("race"), new IndexOption().concurrently(concurrently));
                        assertThat(hasAdvisoryLock(worker)).isFalse();
                        assertThat(worker.getAutoCommit()).isTrue();
                        return result;
                    }
                }));
            }
            start.countDown();
            int created = 0;
            for (var future : futures) {
                if (!future.get(15, TimeUnit.SECONDS).isEmpty()) {
                    created++;
                }
            }
            assertThat(created).isEqualTo(1);
            assertThat(definition("race")).contains("USING btree");
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void lock_timeout_restores_session_and_allows_retry() throws Exception {
        long lockKey = (quoted(SCHEMA) + "." + quoted(table) + "." + index("timeout")).hashCode();
        try (var holder = cosmos.getDataSource().getConnection()) {
            holder.setAutoCommit(false);
            try (var stmt = holder.createStatement()) {
                stmt.execute("SELECT pg_advisory_xact_lock(" + lockKey + ")");
            }
            assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4SingleField(
                    conn, SCHEMA, table, new PGIndexField("timeout"), concurrent(), 100))
                    .isInstanceOf(SQLException.class)
                    .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("55P03");
            assertCleanConnection();
            holder.rollback();
        }
        assertThat(TableUtil.createIndexIfNotExist4SingleField(
                conn, SCHEMA, table, new PGIndexField("timeout"), concurrent())).isNotEmpty();
        assertCleanConnection();
    }

    @Test
    void failed_unique_build_is_propagated_and_invalid_index_requires_explicit_recovery() throws Exception {
        execute("INSERT INTO " + qualifiedTable + " (id, data) VALUES ('a', '{\"email\":\"same\"}'), ('b', '{\"email\":\"same\"}')");
        var field = new PGIndexField("email");
        var option = IndexOption.unique(true).concurrently(true);
        assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, field, option))
                .isInstanceOf(SQLException.class)
                .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("23505");
        assertCleanConnection();

        for (var retryOption : List.of(option, new IndexOption(), new IndexOption().concurrently(false))) {
            assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, field, retryOption))
                    .isInstanceOf(SQLException.class).hasMessageContaining("INVALID").hasMessageContaining("DROP INDEX CONCURRENTLY");
            assertCleanConnection();
        }

        execute("DROP INDEX CONCURRENTLY " + quoted(SCHEMA) + "." + index("email"));
        execute("DELETE FROM " + qualifiedTable + " WHERE id = 'b'");
        assertThat(TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table, field, option)).isNotEmpty();
        assertCleanConnection();
    }

    @Test
    void unsupported_gin_combinations_do_not_change_the_connection() throws Exception {
        assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4SingleField(conn, SCHEMA, table,
                new PGIndexField("tags"), IndexOption.unique(true).gin().concurrently(true)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unique");
        assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4MultiFields(conn, SCHEMA, table,
                List.of(new PGIndexField("a"), new PGIndexField("b")), concurrent().gin()))
                .isInstanceOf(IllegalArgumentException.class);
        assertCleanConnection();
    }

    private static IndexOption concurrent() {
        return new IndexOption().concurrently(true);
    }

    private static String quoted(String name) {
        return TableUtil.checkAndNormalizeValidEntityName(name);
    }

    private String index(String field) {
        return TableUtil.getIndexName(table, field);
    }

    private String definition(String field) throws SQLException {
        return scalar("SELECT pg_get_indexdef('" + quoted(SCHEMA) + "." + index(field) + "'::regclass)");
    }

    private void execute(String sql) throws SQLException {
        try (var stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private String scalar(String sql) throws SQLException {
        try (var stmt = conn.createStatement(); var rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            return rs.getString(1);
        }
    }

    private static boolean hasAdvisoryLock(Connection connection) throws SQLException {
        try (var stmt = connection.createStatement();
             var rs = stmt.executeQuery("SELECT 1 FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'")) {
            return rs.next();
        }
    }

    private void assertCleanConnection() throws SQLException {
        assertThat(conn.getAutoCommit()).isTrue();
        assertThat(scalar("SHOW lock_timeout")).isEqualTo("7s");
        assertThat(hasAdvisoryLock(conn)).isFalse();
        assertThat(scalar("SELECT 1")).isEqualTo("1");
    }
}
