package io.github.thunderz99.cosmos.impl.postgres.util;

import io.github.thunderz99.cosmos.impl.postgres.dto.IndexOption;
import io.github.thunderz99.cosmos.impl.postgres.dto.PGIndexField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TableUtilIndexConnectionTest {
    private Connection conn;
    private Statement stmt;
    private PreparedStatement config;
    private ResultSet indexResult;

    @BeforeEach
    void beforeEach() throws Exception {
        conn = mock(Connection.class);
        stmt = mock(Statement.class);
        config = mock(PreparedStatement.class);
        var timeout = mock(ResultSet.class);
        var lock = mock(ResultSet.class);
        var indexQuery = mock(PreparedStatement.class);
        indexResult = mock(ResultSet.class);
        when(conn.getAutoCommit()).thenReturn(true);
        when(conn.createStatement()).thenReturn(stmt);
        when(stmt.executeQuery("SHOW lock_timeout")).thenReturn(timeout);
        when(timeout.next()).thenReturn(true);
        when(timeout.getString(1)).thenReturn("7s");
        when(stmt.executeQuery(startsWith("SELECT pg_try_advisory_lock("))).thenReturn(lock);
        when(lock.next()).thenReturn(true);
        when(lock.getBoolean(1)).thenReturn(true);
        when(conn.prepareStatement(anyString())).thenReturn(indexQuery);
        when(conn.prepareStatement("SELECT set_config('lock_timeout', ?, false)")).thenReturn(config);
        when(indexQuery.executeQuery()).thenReturn(indexResult);
    }

    @Test
    void option_defaults_to_false_and_can_be_enabled_and_disabled_fluently() {
        var option = IndexOption.unique(true).gin().fieldType("jsonb");
        assertThat(option.concurrently).isFalse();
        assertThat(option.concurrently(true)).isSameAs(option);
        assertThat(option.concurrently).isTrue();
        assertThat(option.concurrently(false)).isSameAs(option);
        assertThat(option.concurrently).isFalse();
        assertThat(option.unique).isTrue();
        assertThat(option.indexMethod).isEqualTo(IndexOption.IndexMethod.GIN);
        assertThat(option.fieldType).isEqualTo("jsonb");
    }

    @Test
    void default_and_explicit_false_keep_ordinary_transactional_ddl() throws Exception {
        var lock = mock(ResultSet.class);
        when(stmt.executeQuery(startsWith("SELECT pg_try_advisory_xact_lock("))).thenReturn(lock);
        when(lock.next()).thenReturn(true);
        when(lock.getBoolean(1)).thenReturn(true);
        when(indexResult.next()).thenReturn(false, true, false, true);
        when(indexResult.getString("tablename")).thenReturn("table");
        when(indexResult.getBoolean("indisvalid")).thenReturn(true);
        for (var option : new IndexOption[]{new IndexOption(), new IndexOption().concurrently(false)}) {
            assertThat(TableUtil.createIndexIfNotExist4SingleField(conn, "schema", "table", new PGIndexField("email"), option))
                    .isEqualTo("schema.idx_table_email_1");
        }
        verify(stmt, never()).execute(contains("CONCURRENTLY"));
        verify(stmt, org.mockito.Mockito.times(2)).execute(startsWith("CREATE  INDEX  IF NOT EXISTS"));
        verify(conn, org.mockito.Mockito.times(2)).commit();
        verify(conn, never()).rollback();
        verify(conn, never()).prepareStatement("SELECT set_config('lock_timeout', ?, false)");
    }

    @Test
    void ordinary_caller_transaction_fails_fast_when_index_lock_is_busy() throws Exception {
        when(conn.getAutoCommit()).thenReturn(false);
        var lock = mock(ResultSet.class);
        when(stmt.executeQuery(startsWith("SELECT pg_try_advisory_xact_lock("))).thenReturn(lock);
        when(lock.next()).thenReturn(true);
        when(lock.getBoolean(1)).thenReturn(false);

        assertThatThrownBy(() -> TableUtil.createIndexIfNotExist4SingleField(
                conn, "schema", "table", new PGIndexField("email"), new IndexOption()))
                .isInstanceOf(SQLException.class).hasMessageContaining("caller transaction")
                .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("55P03");

        verify(stmt, times(1)).executeQuery(startsWith("SELECT pg_try_advisory_xact_lock("));
        verify(stmt, never()).execute(startsWith("SELECT pg_advisory_xact_lock("));
        verify(stmt, never()).execute(startsWith("CREATE "));
        verify(conn, never()).commit();
        // Preserve ordinary creation's existing rollback-on-failure contract, without retrying.
        verify(conn, times(1)).rollback();
        verify(conn, never()).setAutoCommit(true);
    }

    @Test
    void ordinary_caller_transaction_creates_index_when_lock_is_available() throws Exception {
        when(conn.getAutoCommit()).thenReturn(false);
        var lock = mock(ResultSet.class);
        when(stmt.executeQuery(startsWith("SELECT pg_try_advisory_xact_lock("))).thenReturn(lock);
        when(lock.next()).thenReturn(true);
        when(lock.getBoolean(1)).thenReturn(true);
        when(indexResult.next()).thenReturn(false, false, true);
        when(indexResult.getString("tablename")).thenReturn("table");
        when(indexResult.getBoolean("indisvalid")).thenReturn(true);

        assertThat(TableUtil.createIndexIfNotExist4SingleField(
                conn, "schema", "table", new PGIndexField("email"), new IndexOption()))
                .isEqualTo("schema.idx_table_email_1");

        verify(stmt).execute("SET LOCAL lock_timeout = '30000ms'");
        verify(stmt).executeQuery(startsWith("SELECT pg_try_advisory_xact_lock("));
        verify(stmt, never()).execute(startsWith("SELECT pg_advisory_xact_lock("));
        verify(stmt).execute(startsWith("CREATE  INDEX  IF NOT EXISTS"));
        verify(conn).commit();
        verify(conn, never()).rollback();
        verify(conn, never()).setAutoCommit(true);
    }

    @Test
    void ddl_failure_is_preserved_and_session_state_is_restored() throws Exception {
        var failure = new SQLException("build failed", "23505");
        when(stmt.execute(contains("INDEX CONCURRENTLY"))).thenThrow(failure);
        assertThatThrownBy(this::create).isSameAs(failure);
        verify(stmt).execute(startsWith("SELECT pg_advisory_unlock("));
        verify(config).setString(1, "7s");
        verify(conn, never()).setAutoCommit(anyBoolean());
        verify(conn, never()).commit();
        verify(conn, never()).rollback();
        verify(conn, never()).abort(any());
    }

    @Test
    void failure_to_restore_timeout_is_suppressed_and_connection_is_aborted() throws Exception {
        var failure = new SQLException("build failed", "23505");
        var cleanup = new SQLException("restore failed", "08006");
        when(stmt.execute(contains("INDEX CONCURRENTLY"))).thenThrow(failure);
        when(config.execute()).thenReturn(true).thenThrow(cleanup);
        assertThatThrownBy(this::create).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanup);
        verify(stmt).execute(startsWith("SELECT pg_advisory_unlock("));
        verify(conn).abort(any());
    }

    @Test
    void unlock_failure_still_restores_timeout_and_aborts_connection() throws Exception {
        var failure = new SQLException("build failed", "23505");
        var cleanup = new SQLException("unlock failed", "08006");
        when(stmt.execute(contains("INDEX CONCURRENTLY"))).thenThrow(failure);
        when(stmt.execute(startsWith("SELECT pg_advisory_unlock("))).thenThrow(cleanup);
        assertThatThrownBy(this::create).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanup);
        verify(config).setString(1, "7s");
        verify(conn).abort(any());
    }

    @Test
    void failure_to_acquire_lock_restores_timeout_without_unlocking_an_unowned_lock() throws Exception {
        var failure = new SQLException("lock failed", "57014");
        when(stmt.executeQuery(startsWith("SELECT pg_try_advisory_lock("))).thenThrow(failure);
        assertThatThrownBy(this::create).isSameAs(failure);
        verify(stmt, never()).execute(startsWith("SELECT pg_advisory_unlock("));
        verify(config).setString(1, "7s");
        verify(conn, never()).abort(any());
    }

    @Test
    void caller_transaction_is_rejected_before_any_statement_is_created() throws Exception {
        when(conn.getAutoCommit()).thenReturn(false);
        assertThatThrownBy(this::create).isInstanceOf(SQLException.class).hasMessageContaining("autoCommit=true");
        verify(conn, never()).createStatement();
        verify(conn, never()).prepareStatement(anyString());
        verify(conn, never()).commit();
        verify(conn, never()).rollback();
        verify(conn, never()).setAutoCommit(anyBoolean());
    }

    @Test
    void expired_lock_deadline_reports_lock_timeout() {
        assertThatThrownBy(() -> TableUtil.waitForIndexLock(System.nanoTime() - 1, 100))
                .isInstanceOf(SQLException.class)
                .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("55P03");
    }

    @Test
    void interrupted_lock_wait_preserves_thread_interrupt_status() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> TableUtil.waitForIndexLock(System.nanoTime() + 1_000_000_000L, 1_000))
                    .isInstanceOf(SQLException.class)
                    .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo("57014");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    private void create() throws SQLException {
        TableUtil.createIndexIfNotExist4SingleField(conn, "schema", "table", new PGIndexField("email"),
                new IndexOption().concurrently(true));
    }
}
