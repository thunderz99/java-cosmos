package io.github.thunderz99.cosmos;

import java.util.List;

import io.github.thunderz99.cosmos.condition.BucketAggregateFunction;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregate;
import io.github.thunderz99.cosmos.impl.cosmosdb.CosmosDatabaseImpl;
import io.github.thunderz99.cosmos.impl.mongo.MongoDatabaseImpl;
import io.github.thunderz99.cosmos.impl.postgres.PostgresDatabaseImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MultiBucketAggregateFailFastTest {

    @Test
    void aggregateMultiBucket_should_validate_before_database_access() {
        var invalidAggregate = MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null, List.of());
        var databases = List.of(
                new CosmosDatabaseImpl(null, "db"),
                new PostgresDatabaseImpl(null, "db"),
                new MongoDatabaseImpl(null, "db")
        );

        for (var database : databases) {
            assertThatThrownBy(() -> database.aggregateMultiBucket(
                    "collection", invalidAggregate, Condition.filter(), "partition"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("buckets");
        }
    }
}
