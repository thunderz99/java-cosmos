package io.github.thunderz99.cosmos;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.microsoft.azure.documentdb.DocumentClientException;

import io.github.cdimascio.dotenv.Dotenv;

public class CosmosTest {

	static Dotenv dotenv = Dotenv.load();

	@Test
	void testConnectionString() throws DocumentClientException {
		var cosmos = new Cosmos("AccountEndpoint=https://example.azure.com:443/;AccountKey=abcd==;");

		assertThat(cosmos.client).isNotNull();

	}

}
