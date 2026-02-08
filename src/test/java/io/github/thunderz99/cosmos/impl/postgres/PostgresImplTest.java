package io.github.thunderz99.cosmos.impl.postgres;

import io.github.thunderz99.cosmos.impl.mongo.MongoImpl;
import io.github.thunderz99.cosmos.impl.postgres.dto.QueryContext;
import io.github.thunderz99.cosmos.util.EnvUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PostgresImplTest {

    /**
     * local postgres connection string if test against a local setup.
     */
    public static final String LOCAL_CONNECTION_STRING = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres&sslmode=disable";

    String db = "unit_test_db_" + RandomStringUtils.randomAlphanumeric(6);

    String coll = "unit_test_schema_" + RandomStringUtils.randomAlphanumeric(6);

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
            // test with jdbc format
            var pair = PostgresImpl.parseToHikariConfig("jdbc:postgresql://xxx:yyy@pg-local-test1.postgres.database.example.com:5432/java_cosmos");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://pg-local-test1.postgres.database.example.com:5432/java_cosmos");
            assertThat(config.getUsername()).isEqualTo("xxx");
            assertThat(config.getPassword()).isEqualTo("yyy");
            assertThat(account).isEqualTo("pg-local-test1.postgres.database.example.com");
        }

        {
            // test with jdbc format and user/pass as query string
            var pair = PostgresImpl.parseToHikariConfig("jdbc:postgresql://pg-local-test1.postgres.database.example.com:5432/java_cosmos?user=xxx&password=yyy&sslmode=require");
            var config = pair.getLeft();
            var account = pair.getRight();

            assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://pg-local-test1.postgres.database.example.com:5432/java_cosmos?user=xxx&password=yyy&sslmode=require");
            assertThat(config.getUsername()).isEqualTo("xxx");
            assertThat(config.getPassword()).isEqualTo("yyy");
            assertThat(account).isEqualTo("pg-local-test1.postgres.database.example.com");
        }

    }

    @Test
    void createIfNotExist_should_work() throws Exception {

        var cosmos = new PostgresImpl(EnvUtil.getOrDefault("POSTGRES_CONNECTION_STRING", PostgresImplTest.LOCAL_CONNECTION_STRING));
        try {
            var db1 = cosmos.createIfNotExist(db, coll);
            assertThat(db1).isNotNull();
            assertThat(db1.getDatabaseName()).isEqualTo(db);

            var db2 = (PostgresDatabaseImpl)cosmos.getDatabase(db);
            assertThat(db2.getAccount()).isNotEmpty();
            assertThat(db2.getDatabaseName()).isEqualTo(db);

            {
                //QueryContext toJson should work
                var queryContext = QueryContext.create().databaseImpl(db2);
                queryContext.schemaName = coll;
                queryContext.tableName = "Users";

                var json = queryContext.toString();
                assertThat(json).isNotNull().contains(queryContext.schemaName, queryContext.tableName)
                        .doesNotContain("databaseImpl");
            }

        } finally {
            if(cosmos != null) {
                cosmos.deleteCollection(db, coll);
            }
        }

    }


}