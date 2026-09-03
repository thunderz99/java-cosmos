package io.github.thunderz99.cosmos.condition;

import java.util.ArrayList;
import java.util.List;

import io.github.thunderz99.cosmos.dto.RecordData;

/**
 * A request to aggregate one field across multiple independent condition buckets.
 */
public class MultiBucketAggregate extends RecordData {

    public BucketAggregateFunction function;

    public String field;

    public List<ConditionBucket> buckets = new ArrayList<>();

    public MultiBucketAggregate() {
    }

    public static MultiBucketAggregate of(BucketAggregateFunction function, String field,
                                          List<ConditionBucket> buckets) {
        var ret = new MultiBucketAggregate();
        ret.function = function;
        ret.field = field;
        ret.buckets = buckets == null ? null : new ArrayList<>(buckets);
        return ret;
    }
}
