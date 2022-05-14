package io.github.thunderz99.cosmos.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionStringUtilTest {

    @Test
    void parse_connection_string_should_work() {
        {
            var pair = ConnectionStringUtil.parseConnectionString("AccountEndpoint=https://example-dev.documents.azure.com:443/;AccountKey=abcd==;");
            assertThat(pair.getLeft()).isEqualTo("https://example-dev.documents.azure.com:443/");
            assertThat(pair.getRight()).isEqualTo("abcd==");
        }
        {
            var pair = ConnectionStringUtil.parseConnectionString("AccountEndpoint=https://10.211.55.4:8081/;AccountKey=cdef/R+1234/Jw==");
            assertThat(pair.getLeft()).isEqualTo("https://10.211.55.4:8081/");
            assertThat(pair.getRight()).isEqualTo("cdef/R+1234/Jw==");

        }
    }

}