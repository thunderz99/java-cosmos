package io.github.thunderz99.cosmos.impl.postgres;

import io.github.thunderz99.cosmos.impl.mongo.MongoImpl;
import io.github.thunderz99.cosmos.util.EnvUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PostgresImplTest {

    /**
     * local postgres connection string if test against a local setup.
     */
    public static final String LOCAL_CONNECTION_STRING = "postgressql://localhost:5432/test?sslmode=disable";


    String schema = "unit_test_schema_" + RandomStringUtils.randomAlphanumeric(6);

    /**
     * local mongodb connection string if test against a local mongo setup. note that replicaSet is a MUST because we use transaction
     */
    @Test
    void parseToHikariConfig_should_work() {
        {
            // test with no user or pass
            var pair = PostgresImpl.parseToHikariConfig("postgres://localhost:5432/test?sslmode=disable");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/test?sslmode=disable");
            assertThat(config.getUsername()).isNull();
            assertThat(config.getPassword()).isNull();
            assertThat(account).isEqualTo("localhost");
        }
        {
            // test with user and pass
            var pair = PostgresImpl.parseToHikariConfig("postgres://user:pass@test.example.com:5432/test?a=1&b=2");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://test.example.com:5432/test?a=1&b=2");
            assertThat(config.getUsername()).isEqualTo("user");
            assertThat(config.getPassword()).isEqualTo("pass");
            assertThat(account).isEqualTo("test.example.com");
        }
        {
            // test with postgresql:// and user and pass
            var pair = PostgresImpl.parseToHikariConfig("postgresql://user:pass@localhost:5432/database1?sslmode=disable");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/database1?sslmode=disable");
            assertThat(config.getUsername()).isEqualTo("user");
            assertThat(config.getPassword()).isEqualTo("pass");
            assertThat(account).isEqualTo("localhost");
        }
        {
            // test with user only
            var pair = PostgresImpl.parseToHikariConfig("postgres://user@localhost:5432/test?sslmode=disable");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/test?sslmode=disable");
            assertThat(config.getUsername()).isEqualTo("user");
            assertThat(config.getPassword()).isNull();
            assertThat(account).isEqualTo("localhost");
        }
        {
            // test with remote server
            var pair = PostgresImpl.parseToHikariConfig("jdbc:postgresql://xxx:yyy@pg-local-test1.postgres.database.example.com:5432/java_cosmos");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://pg-local-test1.postgres.database.example.com:5432/java_cosmos");
            assertThat(config.getUsername()).isEqualTo("xxx");
            assertThat(config.getPassword()).isEqualTo("yyy");
            assertThat(account).isEqualTo("pg-local-test1.postgres.database.example.com");
        }

    }

    @Test
    void createIfNotExist_should_work() throws Exception {

        var cosmos = new PostgresImpl(EnvUtil.get("POSTGRES_CONNECTION_STRING"));
        try {
            var db1 = cosmos.createIfNotExist(schema, "Users");
            assertThat(db1).isNotNull();
            assertThat(db1.getDatabaseName()).isEqualTo(schema.toLowerCase());

            var db2 = (PostgresDatabaseImpl)cosmos.getDatabase(schema);
            assertThat(db2.getAccount()).isNotEmpty();
            assertThat(db2.getDatabaseName()).isEqualTo(schema.toLowerCase());

        } finally {
            cosmos.deleteDatabase(schema);
        }

    }


}