package io.github.thunderz99.cosmos.impl.mongo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoImplTest {

    @Test
    void extractAccountName_should_work(){
        assertThat(MongoImpl.extractAccountName("mongodb://localhost:27017")).isEqualTo("localhost");
        assertThat(MongoImpl.extractAccountName("mongodb+srv://xxx:yyy@rapid-mongo.test.mongodb.net/")).isEqualTo("rapid-mongo");

    }

}