package io.github.thunderz99.cosmos.rest;

import java.util.Map;

import io.github.thunderz99.cosmos.Cosmos;
import io.github.thunderz99.cosmos.CosmosDatabase;
import io.github.thunderz99.cosmos.util.EnvUtil;
import org.apache.commons.collections4.MapUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RestClientTest {

    static String dbName = "CosmosDB";
    static String coll = "UnitTest";

    RestClient client = new RestClient(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));

    static Cosmos cosmos;
    static CosmosDatabase db;

    @BeforeAll
    static void beforeAll() throws Exception {
        cosmos = new Cosmos(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));
        db = cosmos.createIfNotExist(dbName, coll);
    }


    @Test
    void getDatabase_should_work() throws Exception {
        var result = client.getDatabase(dbName);
        assertThat(result).isNotNull().containsEntry("id", dbName);

    }

    @Test
    void increment_should_work() throws Exception {
        var id = "RestClientTest.increment_should_work.id";
        Map<String, Object> doc = Map.of("id", id, "count", 23);
        var partition = "Sequences";

        try {
            db.upsert(coll, doc, partition);
            var result = client.increment(dbName, coll, id, "/count", partition);
            assertThat(result).isNotNull();
            var data = result.toMap();

            assertThat(MapUtils.getInteger(data, "count")).isEqualTo(1);

        } finally {
            db.delete(coll, id, partition);
        }

    }


}