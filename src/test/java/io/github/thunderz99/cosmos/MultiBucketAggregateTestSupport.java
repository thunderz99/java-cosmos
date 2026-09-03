package io.github.thunderz99.cosmos;

import java.util.List;
import java.util.stream.IntStream;

import io.github.thunderz99.cosmos.condition.BucketAggregateFunction;
import io.github.thunderz99.cosmos.condition.Condition;
import io.github.thunderz99.cosmos.condition.ConditionBucket;
import io.github.thunderz99.cosmos.condition.MultiBucketAggregate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared cross-database assertions for the multi-bucket aggregate contract.
 */
public final class MultiBucketAggregateTestSupport {

    private MultiBucketAggregateTestSupport() {
    }

    public static void assertMultiBucketAggregateWorks(CosmosDatabase db, String coll, String partition)
            throws Exception {
        var buckets = List.of(
                ConditionBucket.of("last-name/Hanks", Condition.filter("fullName.last", "Hanks")),
                ConditionBucket.of("skill-Java-or-Go",
                        Condition.filter("skills ARRAY_CONTAINS_ANY", List.of("Java", "Go"))),
                ConditionBucket.of("empty-bucket", Condition.filter("age >", 100))
        );
        var sharedCondition = Condition.filter("age >=", 12)
                .fields("id")
                .sort("id", "ASC")
                .offset(1)
                .limit(1);

        assertFunction(db, coll, partition, sharedCondition, buckets,
                BucketAggregateFunction.COUNT, null,
                new long[]{2, 2, 0}, new Double[]{2.0, 2.0, 0.0});
        assertFunction(db, coll, partition, sharedCondition, buckets,
                BucketAggregateFunction.SUM, "age",
                new long[]{2, 2, 0}, new Double[]{42.0, 75.0, null});
        assertFunction(db, coll, partition, sharedCondition, buckets,
                BucketAggregateFunction.AVG, "age",
                new long[]{2, 2, 0}, new Double[]{21.0, 37.5, null});
        assertFunction(db, coll, partition, sharedCondition, buckets,
                BucketAggregateFunction.MIN, "age",
                new long[]{2, 2, 0}, new Double[]{12.0, 30.0, null});
        assertFunction(db, coll, partition, sharedCondition, buckets,
                BucketAggregateFunction.MAX, "age",
                new long[]{2, 2, 0}, new Double[]{30.0, 45.0, null});

        var allNullAggregate = MultiBucketAggregate.of(BucketAggregateFunction.SUM, "missingScore",
                List.of(ConditionBucket.of("matched-but-null", Condition.filter("fullName.last", "Hanks"))));
        var allNullResult = db.aggregateMultiBucket(coll, allNullAggregate, sharedCondition, partition);
        assertThat(allNullResult).hasSize(1);
        assertThat(allNullResult.get(0).matched).isEqualTo(2L);
        assertThat(allNullResult.get(0).value).isNull();

        var fortyEightBuckets = IntStream.range(0, 48)
                .mapToObj(i -> ConditionBucket.of("bucket/" + i, Condition.filter("age >=", 12)))
                .toList();
        var fortyEightBucketResult = db.aggregateMultiBucket(coll,
                MultiBucketAggregate.of(BucketAggregateFunction.COUNT, null, fortyEightBuckets),
                Condition.filter(), partition);
        assertThat(fortyEightBucketResult).hasSize(48);
        assertThat(fortyEightBucketResult).allSatisfy(result -> {
            assertThat(result.matched).isEqualTo(3L);
            assertThat(result.value.longValue()).isEqualTo(3L);
        });

        var catchAll = List.of(ConditionBucket.of("catch-all", Condition.filter()));
        assertFunction(db, coll, partition, Condition.filter("age >", 1000), catchAll,
                BucketAggregateFunction.COUNT, null,
                new long[]{0}, new Double[]{0.0});
        assertFunction(db, coll, partition, Condition.filter("age >", 1000), catchAll,
                BucketAggregateFunction.SUM, "age",
                new long[]{0}, new Double[]{null});
    }

    private static void assertFunction(CosmosDatabase db, String coll, String partition,
                                       Condition sharedCondition, List<ConditionBucket> buckets,
                                       BucketAggregateFunction function, String field,
                                       long[] expectedMatched, Double[] expectedValues) throws Exception {
        var aggregate = MultiBucketAggregate.of(function, field, buckets);

        var results = db.aggregateMultiBucket(coll, aggregate, sharedCondition, partition);

        assertThat(results).hasSize(buckets.size());
        assertThat(results).extracting(result -> result.bucketId)
                .containsExactlyElementsOf(buckets.stream().map(bucket -> bucket.bucketId).toList());
        for (var i = 0; i < results.size(); i++) {
            assertThat(results.get(i).matched).isEqualTo(expectedMatched[i]);
            if (expectedValues[i] == null) {
                assertThat(results.get(i).value).isNull();
            } else {
                assertThat(results.get(i).value).isNotNull();
                assertThat(results.get(i).value.doubleValue()).isEqualTo(expectedValues[i]);
            }
        }
    }
}
