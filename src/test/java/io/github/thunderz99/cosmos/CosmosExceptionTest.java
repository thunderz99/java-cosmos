package io.github.thunderz99.cosmos;

import com.mongodb.MongoException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CosmosExceptionTest {

    @Test
    void convertStatusCode_should_work_for_DuplicateKey() {
        assertThat(CosmosException.convertStatusCode(new MongoException("duplicate key"))).isEqualTo(409);
        assertThat(CosmosException.convertStatusCode(new MongoException("DuplicateKey"))).isEqualTo(409);
        assertThat(CosmosException.convertStatusCode(new MongoException("E11000"))).isEqualTo(409);
    }
}