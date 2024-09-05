package io.github.thunderz99.cosmos.impl.cosmosdb;

import java.util.Map;
import java.util.Set;

import io.github.thunderz99.cosmos.util.EnvUtil;
import io.github.thunderz99.cosmos.util.UniqueKeyUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CosmosImplTest {

    static String dbName = "CosmosDB";
    static String coll = "UnitTest";

    @Test
    void testCreateAndDeleteCollection_withUniqueKey() throws Exception {
        var cosmos = new CosmosImpl(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));
        String testColl = "UnitTest2-UniqueKey";
        var id1 = "001";
        var id2 = "002";
        var partition = "Users";
        var db = cosmos.getDatabase(dbName);

        try {
            var uniqueKeyPolicy = UniqueKeyUtil.getUniqueKeyPolicy(Set.of("/_uniqueKey1", "/_uniqueKey2"));
            cosmos.createIfNotExist(dbName, testColl, uniqueKeyPolicy);

            var collection = cosmos.readCollection(dbName, testColl);
            var uniqueKeys = collection.getUniqueKeyPolicy().uniqueKeys;
            assertThat(uniqueKeys).hasSize(2);

            for (var uniqueKey : uniqueKeys) {
                assertThat(uniqueKey.getPaths()).hasSize(1).containsAnyOf("/_uniqueKey1", "/_uniqueKey2");
            }

            var doc1 = db.upsert(testColl, Map.of("id", id1, "_uniqueKey1", "key1", "_uniqueKey2", "key2"), partition);
            assertThat(doc1).isNotNull();
            assertThat(doc1.toMap().getOrDefault("id", "")).isEqualTo(id1);

            assertThatThrownBy(() -> {
                //uniqueKey conflicts with doc1
                db.upsert(testColl, Map.of("id", id2, "_uniqueKey1", "key1", "_uniqueKey2", "key2"), partition);
            }).hasMessageContaining("409");

            var doc2 = db.upsert(testColl, Map.of("id", id2, "_uniqueKey1", "key3", "_uniqueKey2", "key4"), partition);
            assertThat(doc2).isNotNull();
            assertThat(doc2.toMap().getOrDefault("_uniqueKey1", "")).isEqualTo("key3");


        } finally {
            cosmos.deleteCollection(dbName, testColl);
        }

    }

    /**
     * the same case above with unique key is enough. to shorten the unit test time
     *
     * @throws Exception
     */
    @Disabled
    void testCreateAndDeleteCollection() throws Exception {
        var cosmos = new CosmosImpl(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));
        String coll2 = "UnitTest2";
        var id1 = "001";
        var id2 = "002";
        var partition = "Users";
        var db = cosmos.getDatabase(dbName);

        try {
            cosmos.createIfNotExist(dbName, coll2);
            var upserted = db.upsert(coll2, Map.of("id", id2), partition);
            assertThat(upserted).isNotNull();
            assertThat(upserted.toMap().getOrDefault("id", "")).isEqualTo(id2);
            cosmos.deleteCollection(dbName, coll2);

            //coll2 is deleted. but coll still exist
            upserted = db.upsert(coll, Map.of("id", id1), partition);
            assertThat(upserted.toMap().getOrDefault("id", "")).isEqualTo(id1);
        } finally {
            cosmos.deleteCollection(dbName, coll2);
            db.delete(coll, id1, partition);
            db.delete(coll2, id2, partition);
        }

    }

    @Test
    void cosmos_account_should_be_get() {
        var cosmos = new CosmosImpl(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));
        assertThat(cosmos.getAccount()).isEqualTo("rapid-cosmos");
    }

    @Test
    void getClientV4_should_work() {
        var cosmos = new CosmosImpl(EnvUtil.get("COSMOSDB_CONNECTION_STRING"));
        assertThat(cosmos.getClientV4()).isNotNull();

        var dbs = cosmos.getClientV4().readAllDatabases();
        assertThat(dbs).isNotEmpty();

    }

    @Test
    void extractAccountName_should_work() {

        // Test with a standard endpoint
        String accountName = CosmosImpl.extractAccountName("https://example.documents.azure.com:443/");
        assertThat(accountName).isEqualTo("example");

        // Test with an endpoint that has a different subdomain
        accountName = CosmosImpl.extractAccountName("https://different.documents.azure.com:443/");
        assertThat(accountName).isEqualTo("different");

        // Test with an endpoint that has an unusual subdomain format
        accountName = CosmosImpl.extractAccountName("https://sub-domain.example.documents.azure.com:443/");
        assertThat(accountName).isEqualTo("sub-domain");

        // Test with an endpoint without the standard port
        accountName = CosmosImpl.extractAccountName("https://example.documents.azure.com/");
        assertThat(accountName).isEqualTo("example");

        // Test with an invalid URL to ensure proper error handling or return value
        accountName = CosmosImpl.extractAccountName("https:///incorrect-url-format");
        assertThat(accountName).isEmpty();
    }

}