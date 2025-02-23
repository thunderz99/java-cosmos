package io.github.thunderz99.cosmos;

import com.mongodb.MongoException;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import static org.assertj.core.api.Assertions.assertThat;

class CosmosExceptionTest {

    @Test
    void convertStatusCode_should_work_for_DuplicateKey() {
        assertThat(CosmosException.convertStatusCode(new MongoException("duplicate key"))).isEqualTo(409);
        assertThat(CosmosException.convertStatusCode(new MongoException("DuplicateKey"))).isEqualTo(409);
        assertThat(CosmosException.convertStatusCode(new MongoException("E11000"))).isEqualTo(409);
    }

    @Test
    void convertStatusCode_should_work_for_postgres() {
        // 409
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.UNIQUE_VIOLATION))).isEqualTo(409);

        // 429
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.CONNECTION_UNABLE_TO_CONNECT))).isEqualTo(429);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.CONNECTION_DOES_NOT_EXIST))).isEqualTo(429);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.CONNECTION_FAILURE))).isEqualTo(429);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.CONNECTION_REJECTED))).isEqualTo(429);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.CONNECTION_FAILURE_DURING_TRANSACTION))).isEqualTo(429);

        // 400
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.BAD_DATETIME_FORMAT))).isEqualTo(400);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.NOT_NULL_VIOLATION))).isEqualTo(400);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.DATA_TYPE_MISMATCH))).isEqualTo(400);

        // 500
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.OBJECT_IN_USE))).isEqualTo(500);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.OUT_OF_MEMORY))).isEqualTo(500);
        assertThat(CosmosException.convertStatusCode(new PSQLException("msg", PSQLState.SYSTEM_ERROR))).isEqualTo(500);


    }
}