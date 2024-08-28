package io.github.thunderz99.cosmos;

import com.mongodb.MongoException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CosmosExceptionTest {

    @Test
    void convertStatusCode_should_work_for_DuplicateKey() {
        var me = new MongoException("DuplicateKey E11000");
        assertThat(CosmosException.convertStatusCode(me)).isEqualTo(409);
    }
}