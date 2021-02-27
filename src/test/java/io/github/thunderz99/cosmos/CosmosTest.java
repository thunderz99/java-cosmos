package io.github.thunderz99.cosmos;

import org.junit.jupiter.api.Test;

import io.github.cdimascio.dotenv.Dotenv;

import static org.assertj.core.api.Assertions.assertThat;

public class CosmosTest {

	static Dotenv dotenv = Dotenv.load();

	@Test
	void parse_connection_string_should_work() {
		var pair = Cosmos.parseConnectionString("AccountEndpoint=https://example-dev.documents.azure.com:443/;AccountKey=abcd==;");
		assertThat(pair.getLeft()).isEqualTo("https://example-dev.documents.azure.com:443/");
		assertThat(pair.getRight()).isEqualTo("abcd==");

		var account = Cosmos.parseAcount(pair.getLeft());
		assertThat(account).isEqualTo("example-dev");
	}

}
