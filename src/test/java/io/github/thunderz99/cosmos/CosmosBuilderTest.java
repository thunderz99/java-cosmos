package io.github.thunderz99.cosmos;

import io.github.thunderz99.cosmos.impl.postgres.dto.PostgresHikariOptions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CosmosBuilderTest {

    @Test
    void withHikariMaximumPoolSize_should_initialize_options_and_set_value() {
        var builder = new CosmosBuilder();

        builder.withHikariMaximumPoolSize(42);

        assertThat(builder.postgresHikariOptions).isNotNull();
        assertThat(builder.postgresHikariOptions.maximumPoolSize).isEqualTo(42);
    }

    @Test
    void withCustomHikariSettings_should_set_given_options() {
        var builder = new CosmosBuilder();
        var options = new PostgresHikariOptions().withMaximumPoolSize(33);

        builder.withCustomHikariSettings(options);

        assertThat(builder.postgresHikariOptions).isSameAs(options);
        assertThat(builder.postgresHikariOptions.maximumPoolSize).isEqualTo(33);
    }

    @Test
    void withHikariMaximumPoolSize_should_override_maximumPoolSize_in_existing_options() {
        var builder = new CosmosBuilder();
        var options = new PostgresHikariOptions().withMaximumPoolSize(20);

        builder.withCustomHikariSettings(options)
                .withHikariMaximumPoolSize(99);

        assertThat(builder.postgresHikariOptions).isSameAs(options);
        assertThat(builder.postgresHikariOptions.maximumPoolSize).isEqualTo(99);
    }

    @Test
    void withCustomHikariSettings_should_replace_options_set_by_hikariMaximumPoolSize() {
        var builder = new CosmosBuilder();
        var options = new PostgresHikariOptions().withMaximumPoolSize(15);

        builder.withHikariMaximumPoolSize(88)
                .withCustomHikariSettings(options);

        assertThat(builder.postgresHikariOptions).isSameAs(options);
        assertThat(builder.postgresHikariOptions.maximumPoolSize).isEqualTo(15);
    }
}
