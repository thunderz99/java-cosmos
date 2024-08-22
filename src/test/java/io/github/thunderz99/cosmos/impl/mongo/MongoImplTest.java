package io.github.thunderz99.cosmos.impl.mongo;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoImplTest {

    /**
     * local mongodb connection string if test against a local mongo setup. note that replicaSet is a MUST because we use transaction
     */
    public static final String LOCAL_CONNECTION_STRING = "mongodb://localhost:27017/?replicaSet=rs0";

    @Test
    void extractAccountName_should_work() {
        assertThat(MongoImpl.extractAccountName(LOCAL_CONNECTION_STRING)).isEqualTo("localhost");
        assertThat(MongoImpl.extractAccountName("mongodb+srv://xxx:yyy@rapid-mongo.test.mongodb.net/")).isEqualTo("rapid-mongo");

    }

}