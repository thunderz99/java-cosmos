package io.github.thunderz99.cosmos;

import java.util.Map;

import io.github.thunderz99.cosmos.util.EnvUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CosmosTest {

    static String dbName = "CosmosDB";


    @Test
    void testCreateAndDeleteCollection_should_work_for_cosmosdb() throws Exception {

        var collName = "testCreateAndDeleteCollection_" + RandomStringUtils.randomAlphanumeric(3);

        var cosmos = new CosmosBuilder().withDatabaseType("cosmosdb").withConnectionString(EnvUtil.get("COSMOSDB_CONNECTION_STRING")).build();

        var db = cosmos.createIfNotExist(dbName, collName);

        var id = "testCreateAndDeleteCollection_should_work_for_cosmosdb";
        var partition = "testCreateAndDeleteCollection";

        try {
            var upserted = db.upsert(collName, Map.of("id", id), partition);
            assertThat(upserted).isNotNull();
            assertThat(upserted.toMap().getOrDefault("id", "")).isEqualTo(id);
        } finally {
            cosmos.deleteCollection(dbName, collName);
            assertThat(cosmos.readCollection(dbName, collName)).isNull();
        }
    }

    @Test
    void testCreateAndDeleteCollection_should_work_for_mongodb() throws Exception {

        var collName = "test_mongo_" + RandomStringUtils.randomAlphanumeric(3);

        var cosmos = new CosmosBuilder().withDatabaseType("mongodb")
                .withConnectionString(EnvUtil.getOrDefault("MONGODB_CONNECTION_STRING", "mongodb://localhost:27017"))
                .build();

        var db = cosmos.createIfNotExist(dbName, collName);

        var id = "testCreateAndDeleteCollection_should_work_for_cosmosdb";
        var partition = "testCreateAndDeleteCollection";

        try {
            var upserted = db.upsert(collName, Map.of("id", id), partition);
            assertThat(upserted).isNotNull();
            assertThat(upserted.toMap().getOrDefault("id", "")).isEqualTo(id);

            var read = db.read(collName, id, partition);
            assertThat(read.toMap()).isNotNull().containsEntry("id", id);

        } finally {
            cosmos.deleteCollection(dbName, collName);
            cosmos.deleteCollection(collName, partition);
        }
    }


}
