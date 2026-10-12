package io.github.thunderz99.cosmos;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.zaxxer.hikari.HikariDataSource;
import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosImpl;
import io.github.thunderz99.cosmos.impl.mongo.MongoImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresImpl;
import io.github.thunderz99.cosmos.impl.postgres.util.PGSortUtil;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class ClientLifecycleContractTest {

    private static final List<String> DATABASE_TYPES = List.of("cosmosdb", "mongodb", "postgres");

    @Test
    void builder_defaults_to_automatic_hook_for_every_database() throws Exception {
        for (var dbType : DATABASE_TYPES) {
            var runtime = mock(Runtime.class);
            try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
                runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
                var cosmos = builder(dbType).build();
                var hook = ArgumentCaptor.forClass(Thread.class);
                verify(runtime).addShutdownHook(hook.capture());
                hook.getValue().run();
                cosmos.closeClient();
                clients.verifyClosed(dbType, 1);
            }
        }
    }

    @Test
    void disabled_hook_still_allows_explicit_close_for_every_database() throws Exception {
        for (var dbType : DATABASE_TYPES) {
            var runtime = mock(Runtime.class);
            try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
                runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
                var cosmos = builder(dbType).withRegisterShutdownHook(false).build();
                clients.verifyClosed(dbType, 0);
                verifyNoInteractions(runtime);
                cosmos.closeClient();
                cosmos.closeClient();
                clients.verifyClosed(dbType, 1);
                verifyNoInteractions(runtime);
            }
        }
    }

    @Test
    void explicitly_enabled_hook_is_removed_after_manual_close_for_every_database() throws Exception {
        for (var dbType : DATABASE_TYPES) {
            var runtime = mock(Runtime.class);
            try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
                runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
                var cosmos = builder(dbType).withRegisterShutdownHook(false).withRegisterShutdownHook(true).build();
                var hook = ArgumentCaptor.forClass(Thread.class);
                verify(runtime).addShutdownHook(hook.capture());
                cosmos.closeClient();
                verify(runtime).removeShutdownHook(hook.getValue());
                hook.getValue().run();
                clients.verifyClosed(dbType, 1);
            }
        }
    }

    @Test
    void all_existing_constructors_keep_automatic_hooks() throws Exception {
        var runtime = mock(Runtime.class);
        try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            var cosmosClients = List.<Cosmos>of(
                    new CosmosImpl(connectionString("cosmosdb")),
                    new CosmosImpl(connectionString("cosmosdb"), List.of("Japan East")),
                    new MongoImpl(connectionString("mongodb")),
                    new MongoImpl(connectionString("mongodb"), true, true),
                    new PostgresImpl(connectionString("postgres")),
                    new PostgresImpl(connectionString("postgres"), true, true, PGSortUtil.COLLATE_C),
                    new PostgresImpl(connectionString("postgres"), true, true, PGSortUtil.COLLATE_C, null),
                    new PostgresImpl(connectionString("postgres"), true, true, PGSortUtil.COLLATE_C, null, 1200));
            verify(runtime, times(cosmosClients.size())).addShutdownHook(any(Thread.class));
            assertThat(((MongoImpl) cosmosClients.get(2)).getClient()).isSameAs(clients.mongoClient);
            assertThat(((PostgresImpl) cosmosClients.get(7)).getInitializationLockTimeoutMs()).isEqualTo(1200);
            for (var cosmos : cosmosClients) {
                cosmos.closeClient();
            }
            verify(runtime, times(cosmosClients.size())).removeShutdownHook(any(Thread.class));
        }
    }

    @Test
    void failed_close_is_propagated_and_retryable_for_every_database() throws Exception {
        for (var dbType : DATABASE_TYPES) {
            try (var clients = new ClientMocks()) {
                var cosmos = builder(dbType).withRegisterShutdownHook(false).build();
                var failure = new IllegalStateException("close failed");
                clients.failFirstClose(dbType, failure);
                assertThatThrownBy(cosmos::closeClient).isSameAs(failure);
                cosmos.closeClient();
                cosmos.closeClient();
                clients.verifyClosed(dbType, 2);
            }
        }
    }

    @Test
    void failed_hook_registration_cleans_up_every_created_client() throws Exception {
        for (var dbType : DATABASE_TYPES) {
            var runtime = mock(Runtime.class);
            var failure = new IllegalStateException("shutdown already started");
            try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
                runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
                doThrow(failure).when(runtime).addShutdownHook(any(Thread.class));
                assertThatThrownBy(() -> builder(dbType).build()).isSameAs(failure);
                clients.verifyClosed(dbType, 1);
                verify(runtime, never()).removeShutdownHook(any(Thread.class));
            }
        }
    }

    @Test
    void mongodb_preflight_failure_cleans_up_client_without_registering_hook() throws Exception {
        var runtime = mock(Runtime.class);
        var failure = new IllegalStateException("ping failed");
        var cleanupFailure = new IllegalStateException("cleanup failed");
        try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            when(clients.mongoDatabase.runCommand(any(Document.class))).thenThrow(failure);
            doThrow(cleanupFailure).when(clients.mongoClient).close();
            assertThatThrownBy(() -> builder("mongodb").build()).isSameAs(failure);
            assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
            clients.verifyClosed("mongodb", 1);
            verifyNoInteractions(runtime);
        }
    }

    @Test
    void postgres_preflight_failure_cleans_up_pool_without_registering_hook() throws Exception {
        var runtime = mock(Runtime.class);
        var failure = new SQLException("ping failed");
        try (var clients = new ClientMocks(); var runtimeMock = mockStatic(Runtime.class)) {
            runtimeMock.when(Runtime::getRuntime).thenReturn(runtime);
            clients.postgresPreflightFailure = failure;
            assertThatThrownBy(() -> builder("postgres").build()).isInstanceOf(CosmosException.class)
                    .hasMessage("Ping failed for postgres db");
            clients.verifyClosed("postgres", 1);
            verifyNoInteractions(runtime);
        }
    }

    private static CosmosBuilder builder(String dbType) {
        return new CosmosBuilder().withDatabaseType(dbType).withConnectionString(connectionString(dbType));
    }

    private static String connectionString(String dbType) {
        switch (dbType) {
            case "cosmosdb":
                return "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=dGVzdA==;";
            case "mongodb":
                return "mongodb://localhost:27017";
            case "postgres":
                return "postgres://localhost:5432/test?sslmode=disable";
            default:
                throw new IllegalArgumentException(dbType);
        }
    }

    private static class ClientMocks implements AutoCloseable {
        final CosmosClient cosmosClient = mock(CosmosClient.class);
        final MongoClient mongoClient = mock(MongoClient.class);
        final MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        final MockedStatic<MongoClients> mongoFactory;
        final MockedConstruction<CosmosClientBuilder> cosmosBuilder;
        final MockedConstruction<HikariDataSource> postgresPools;
        SQLException postgresPreflightFailure;

        @SuppressWarnings("unchecked")
        ClientMocks() throws SQLException {
            when(mongoClient.getDatabase("admin")).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any(Document.class))).thenReturn(new Document("ok", 1));
            ListDatabasesIterable<Document> databases = mock(ListDatabasesIterable.class);
            when(mongoClient.listDatabases()).thenReturn(databases);
            when(databases.into(any(Collection.class))).thenAnswer(invocation -> invocation.getArgument(0));
            mongoFactory = mockStatic(MongoClients.class);
            mongoFactory.when(() -> MongoClients.create(anyString())).thenReturn(mongoClient);
            cosmosBuilder = mockConstruction(CosmosClientBuilder.class, withSettings().defaultAnswer(RETURNS_SELF),
                    (builder, context) -> when(builder.buildClient()).thenReturn(cosmosClient));
            postgresPools = mockConstruction(HikariDataSource.class, (pool, context) -> {
                when(pool.getConnection()).thenAnswer(invocation -> {
                    if (postgresPreflightFailure != null) {
                        throw postgresPreflightFailure;
                    }
                    var connection = mock(Connection.class);
                    when(connection.isValid(2)).thenReturn(true);
                    return connection;
                });
            });
        }

        void verifyClosed(String dbType, int count) {
            switch (dbType) {
                case "cosmosdb":
                    verify(cosmosClient, times(count)).close();
                    break;
                case "mongodb":
                    verify(mongoClient, times(count)).close();
                    break;
                case "postgres":
                    verify(postgresPools.constructed().get(0), times(count)).close();
                    break;
                default:
                    throw new IllegalArgumentException(dbType);
            }
        }

        void failFirstClose(String dbType, RuntimeException failure) {
            switch (dbType) {
                case "cosmosdb":
                    doThrow(failure).doNothing().when(cosmosClient).close();
                    break;
                case "mongodb":
                    doThrow(failure).doNothing().when(mongoClient).close();
                    break;
                case "postgres":
                    doThrow(failure).doNothing().when(postgresPools.constructed().get(0)).close();
                    break;
                default:
                    throw new IllegalArgumentException(dbType);
            }
        }

        @Override
        public void close() {
            postgresPools.close();
            cosmosBuilder.close();
            mongoFactory.close();
        }
    }
}
