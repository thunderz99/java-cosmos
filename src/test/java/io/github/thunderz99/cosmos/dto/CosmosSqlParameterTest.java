package io.github.thunderz99.cosmos.dto;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CosmosSqlParameterTest {

    @Test
    public void get_should_work() {
        // Setup
        String paramName = "testParamName";
        Object paramValue = "testParamValue";
        CosmosSqlParameter param = new CosmosSqlParameter(paramName, paramValue);

        // Execute & Verify
        assertThat(param.get("name")).isEqualTo(paramName);
        assertThat(param.get("value")).isEqualTo(paramValue);
        assertThat(param.get("unexpected")).isNull();
    }

    @Test
    public void getString_should_work() {
        // Setup
        String paramName = "testParamName";
        Object paramValue = 123; // Using a non-String value to test conversion to String
        CosmosSqlParameter param = new CosmosSqlParameter(paramName, paramValue);

        // Execute & Verify
        assertThat(param.getString("name")).isEqualTo(paramName);
        assertThat(param.getString("value")).isEqualTo(paramValue.toString());
        assertThat(param.getString("unexpected")).isNull();
    }
}
