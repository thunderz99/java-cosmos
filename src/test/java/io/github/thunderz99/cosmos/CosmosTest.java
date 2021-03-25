package io.github.thunderz99.cosmos;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.microsoft.azure.documentdb.DocumentClientException;

import io.github.cdimascio.dotenv.Dotenv;

import java.util.Map;

public class CosmosTest {

	static Dotenv dotenv = Dotenv.load();

	static String dbName = "CosmosDB";
	static String coll = "UnitTest";


	@Test
	void testConnectionString() throws DocumentClientException {
		var cosmos = new Cosmos("AccountEndpoint=https://example.azure.com:443/;AccountKey=abcd==;");

		assertThat(cosmos.client).isNotNull();
	}

	@Test
	void testCreateAndDeleteCollection() throws Exception {
		var cosmos = new Cosmos(dotenv.get("COSMOSDB_CONNECTION_STRING"));
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

}
